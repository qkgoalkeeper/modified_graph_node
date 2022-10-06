use crate::subgraph::context::IndexingContext;
use crate::subgraph::error::BlockProcessingError;
use crate::subgraph::inputs::IndexingInputs;
use crate::subgraph::state::IndexingState;
use crate::subgraph::stream::new_block_stream;
use atomic_refcell::AtomicRefCell;
use futures01::sync::mpsc::Receiver;
use graph::blockchain::block_stream::{BlockStreamEvent, BlockWithTriggers, FirehoseCursor};
use graph::blockchain::{Block, Blockchain, DataSource, TriggerFilter as _};
use graph::components::{
    store::ModificationsAndCache,
    subgraph::{CausalityRegion, MappingError, ProofOfIndexing, SharedProofOfIndexing},
};
use graph::data::store::scalar::Bytes;
use graph::data::subgraph::{
    schema::{SubgraphError, SubgraphHealth, POI_OBJECT},
    SubgraphFeature,
};
use graph::prelude::*;
use graph::util::{backoff::ExponentialBackoff, lfu_cache::LfuCache};
use std::convert::TryFrom;

use std::sync::{Arc};
use tokio::sync::Mutex;
use std::thread;
use std::time::{Duration, Instant};

use futures03::channel::oneshot::channel;

use super::state;

pub const MINUTE: Duration = Duration::from_secs(60);

const SKIP_PTR_UPDATES_THRESHOLD: Duration = Duration::from_secs(60 * 5);


pub struct SubgraphRunner<C, T>
where
    C: Blockchain,
    T: RuntimeHostBuilder<C>,
{
    ctx: Arc<Mutex<IndexingContext<C, T>>>,
    inputs: Arc<Mutex<IndexingInputs<C>>>,
    state: Arc<Mutex<IndexingState>>,
    logger: Arc<Mutex<Logger>>,
    metrics: Arc<Mutex<RunnerMetrics>>,
}



pub async fn run<C: Blockchain,T:RuntimeHostBuilder<C>>(mut arc_self:SubgraphRunner<C,T>) -> Result<(), Error> {
    // If a subgraph failed for deterministic reasons, before start indexing, we first
    // revert the deployment head. It should lead to the same result since the error was
    // deterministic.
    {
        let self_inputs_copy = arc_self.inputs.clone();
        let self_inputs_mutex = self_inputs_copy.lock().await;
        if let Some(current_ptr) = self_inputs_mutex.store.block_ptr() {
            if let Some(parent_ptr) = self_inputs_mutex
                .triggers_adapter
                .parent_ptr(&current_ptr)
                .await?
            {
                // This reverts the deployment head to the parent_ptr if
                // deterministic errors happened.
                //
                // There's no point in calling it if we have no current or parent block
                // pointers, because there would be: no block to revert to or to search
                // errors from (first execution).
                let _outcome = self_inputs_mutex
                    .store
                    .unfail_deterministic_error(&current_ptr, &parent_ptr)
                    .await?;
            }
        }
    }

    loop {
        
        let (mut blockstream_now,block_stream_cancel_handle_now) = {
            let self_inputs_copy = arc_self.inputs.clone();
            let self_inputs_mutex = self_inputs_copy.lock().await;

            let self_ctx_copy = arc_self.ctx.clone();
            let self_ctx_mutex = self_ctx_copy.lock().await;

            let self_logger_copy = arc_self.logger.clone();
            let self_logger_mutex = self_logger_copy.lock().await;

            debug!(self_logger_mutex, "Starting or restarting subgraph");

            let block_stream_canceler = CancelGuard::new();
            let block_stream_cancel_handle = block_stream_canceler.handle();


            let mut block_stream = new_block_stream(&self_inputs_mutex, &self_ctx_mutex.filter)
                .await?
                .map_err(CancelableError::Error)
                .cancelable(&block_stream_canceler, || Err(CancelableError::Cancel));

            // Keep the stream's cancel guard around to be able to shut it down when the subgraph
            // deployment is unassigned
            self_ctx_mutex
                .instances
                .write()
                .unwrap()
                .insert(self_inputs_mutex.deployment.id, block_stream_canceler);

            debug!(self_logger_mutex, "Starting block stream");
        
            // Process events from the stream as long as no restart is needed

            (block_stream,block_stream_cancel_handle)
        };

        loop{

            let mut events = Vec::new();

            {


                let self_metrics_copy = arc_self.metrics.clone();
                let self_metrics_mutex = self_metrics_copy.lock().await;




                let _section = self_metrics_mutex.stream.stopwatch.start_section("scan_blocks");
                let mut events_queue = Vec::new();
                for _i in 0..4{
                    let event = {
                        blockstream_now.next().await
                    };
                    match event{
                        Some(t)=>{events_queue.push(Some(t));},
                        _ =>{}
                    }
                    
                }

                for _i in 0..events_queue.len(){
                    let event_reverse = events_queue.pop().unwrap();
                    events.push(event_reverse);


                }
            }

            
            let mut handles = vec![];
            let mut action_for_return:Arc<Mutex<Vec<Result<Action,_>>>> = Arc::new(Mutex::new(Vec::new()));

            for _i in 0..events.len(){
                
                let self_ctx_copy = arc_self.ctx.clone();
                let self_inputs_copy = arc_self.inputs.clone();
                let self_state_copy = arc_self.state.clone();
                let self_logger_copy = arc_self.logger.clone();
                let self_metrics_copy = arc_self.metrics.clone();


                let event = events.pop().unwrap();

                let block_stream_cancel_handle_now_copy = block_stream_cancel_handle_now.clone();


                // let mut self_mutex = self_copy.lock().await;

                // let action = self_mutex.handle_stream_event(event, &block_stream_cancel_handle_now_copy).await?;

                // action_for_return.push(action);
                // state
                // input
                // meetr


                let action_for_return_clone = action_for_return.clone();
                let handle = tokio::spawn(async move {



                            // TODO: move cancel handle to the Context
                        // This will require some code refactor in how the BlockStream is created
                    let action = handle_stream_event(self_ctx_copy,self_inputs_copy,self_state_copy,self_logger_copy,self_metrics_copy,event, &block_stream_cancel_handle_now_copy)
                        .await;
                    
                    let mut action_for_return_mutex = action_for_return_clone.lock().await;
                    action_for_return_mutex.push(action);    

                });

                handles.push(handle);
            }

            for handle in handles{
                let result = handle.await?;
            }


            let mut action_number = 0;

            {
                let self_inputs_copy = arc_self.inputs.clone();
                let self_inputs_mutex = self_inputs_copy.lock().await;
                let self_logger_copy = arc_self.logger.clone();
                let self_logger_mutex = self_logger_copy.lock().await;

                let mut action_for_return_mutex = action_for_return.lock().await;
                for i in 0..action_for_return_mutex.len(){
                    let temp_action = action_for_return_mutex.pop().unwrap().unwrap();
                    match temp_action{
                        Action::Continue => continue,
                        Action::Stop => {
                            info!(self_logger_mutex, "Stopping subgraph");
                            self_inputs_mutex.store.flush().await?;
                            return Ok(());
                        }
                        Action::Restart => {action_number=1;break;},

                    }

                }
            
            }

            if action_number==1{
                break;
            } 
        }
    }
}


impl<C, T> SubgraphRunner<C, T>
where
    C: Blockchain,
    T: RuntimeHostBuilder<C>,
{
    pub fn new(
        inputs: Arc<Mutex<IndexingInputs<C>>>,
        ctx: Arc<Mutex<IndexingContext<C, T>>>,
        state: Arc<Mutex<IndexingState>>,
        logger: Arc<Mutex<Logger>>,
        metrics: Arc<Mutex<RunnerMetrics>>,
    ) -> Self {
        Self {
            inputs,
            ctx,
            state,
            logger,
            metrics,
        }
    }

}


    /// Processes a block and returns the updated context and a boolean flag indicating
    /// whether new dynamic data sources have been added to the subgraph.
async fn process_block<C: Blockchain,T:RuntimeHostBuilder<C>>(
    self_ctx_copy: Arc<Mutex<IndexingContext<C, T>>>,
    self_inputs_copy: Arc<Mutex<IndexingInputs<C>>>,
    self_state_copy: Arc<Mutex<IndexingState>>,
    self_logger_copy: Arc<Mutex<Logger>>,
    self_metrics_copy: Arc<Mutex<RunnerMetrics>>,
    block_stream_cancel_handle: &CancelHandle,
    block: BlockWithTriggers<C>,
    firehose_cursor: FirehoseCursor,
) -> Result<Action, BlockProcessingError> {
    let triggers = block.trigger_data;
    let block = Arc::new(block.block);
    let block_ptr = block.ptr();


    let logger = {

        let self_logger_mutex = self_logger_copy.lock().await;
        let logger = self_logger_mutex.new(o!(
                "block_number" => format!("{:?}", block_ptr.number),
                "block_hash" => format!("{}", block_ptr.hash)
        ));

        if triggers.len() == 1 {
            debug!(&logger, "1 candidate trigger in this block");
        } else {
            debug!(
                &logger,
                "{} candidate triggers in this block",
                triggers.len()
            );
        }
        logger
    };

    let (ctx_instance_poi_version,causality_region) = 
    {
        let self_ctx_mutex = self_ctx_copy.lock().await;
        // There are currently no other causality regions since offchain data is not supported.
        let causality_region = CausalityRegion::from_network(self_ctx_mutex.instance.network());
        (self_ctx_mutex.instance.poi_version.clone(),causality_region)
    };




    let proof_of_indexing={
        let self_inputs_mutex = self_inputs_copy.lock().await;

        let proof_of_indexing = if self_inputs_mutex.store.supports_proof_of_indexing().await? {
            Some(Arc::new(AtomicRefCell::new(ProofOfIndexing::new(
                block_ptr.number,
                ctx_instance_poi_version,
            ))))
        } else {
            None
        };
        proof_of_indexing
    };

    let self_ctx_copy_copy = self_ctx_copy.clone();
    let self_inputs_copy_copy = self_inputs_copy.clone();
    let self_state_copy_copy = self_state_copy.clone();
    let self_logger_copy_copy = self_logger_copy.clone();
    let self_metrics_copy_copy = self_metrics_copy.clone();

    // Process events one after the other, passing in entity operations
    // collected previously to every new event being processed
    let mut block_state = match 
        process_triggers(self_ctx_copy_copy,self_inputs_copy_copy,self_state_copy_copy,self_logger_copy_copy,self_metrics_copy_copy,&proof_of_indexing, &block, triggers, &causality_region)
        .await
    {
        // Triggers processed with no errors or with only deterministic errors.
        Ok(block_state) => block_state,

        // Some form of unknown or non-deterministic error ocurred.
        Err(MappingError::Unknown(e)) => return Err(BlockProcessingError::Unknown(e)),
        Err(MappingError::PossibleReorg(e)) => {
            info!(logger,
                "Possible reorg detected, retrying";
                "error" => format!("{:#}", e),
            );

            // In case of a possible reorg, we want this function to do nothing and restart the
            // block stream so it has a chance to detect the reorg.
            //
            // The `state` is unchanged at this point, except for having cleared the entity cache.
            // Losing the cache is a bit annoying but not an issue for correctness.
            //
            // See also b21fa73b-6453-4340-99fb-1a78ec62efb1.
            return Ok(Action::Restart);
        }
    };

    // If new data sources have been created, and static filters are not in use, it is necessary
    // to restart the block stream with the new filters.
    let needs_restart = 
    {
        let self_inputs_mutex = self_inputs_copy.lock().await;
        let needs_restart = block_state.has_created_data_sources() && !self_inputs_mutex.static_filters;
        needs_restart
    };
    // This loop will:
    // 1. Instantiate created data sources.
    // 2. Process those data sources for the current block.
    // Until no data sources are created or MAX_DATA_SOURCES is hit.

    // Note that this algorithm processes data sources spawned on the same block _breadth
    // first_ on the tree implied by the parent-child relationship between data sources. Only a
    // very contrived subgraph would be able to observe this.
    while block_state.has_created_data_sources() {

        let self_ctx_copy_copy_1 = self_ctx_copy.clone();

        let self_inputs_copy_copy_1 = self_inputs_copy.clone();

        let self_logger_copy_copy_1 = self_logger_copy.clone();
        let self_metrics_copy_copy_1 = self_metrics_copy.clone();



        // Instantiate dynamic data sources, removing them from the block state.
        let (data_sources, runtime_hosts) =
            create_dynamic_data_sources(self_ctx_copy_copy_1,self_inputs_copy_copy_1,self_logger_copy_copy_1,self_metrics_copy_copy_1,block_state.drain_created_data_sources()).await?;

        let filter = C::TriggerFilter::from_data_sources(data_sources.iter());



        // Reprocess the triggers from this block that match the new data sources

        let block_with_triggers = {


            let self_inputs_mutex = self_inputs_copy.lock().await;

            let block_with_triggers = self_inputs_mutex
            .triggers_adapter
            .triggers_in_block(&logger, block.as_ref().clone(), &filter)
            .await?;

            block_with_triggers
        };


        let triggers = block_with_triggers.trigger_data;


        if triggers.len() == 1 {
            info!(
                &logger,
                "1 trigger found in this block for the new data sources"
            );
        } else if triggers.len() > 1 {
            info!(
                &logger,
                "{} triggers found in this block for the new data sources",
                triggers.len()
            );
        }


        let self_ctx_copy_copy_2 = self_ctx_copy.clone();
        let self_logger_copy_copy_2 = self_logger_copy.clone();

        // Add entity operations for the new data sources to the block state
        // and add runtimes for the data sources to the subgraph instance.
        persist_dynamic_data_sources(self_ctx_copy_copy_2,self_logger_copy_copy_2,&mut block_state.entity_cache, data_sources);

        // Process the triggers in each host in the same order the
        // corresponding data sources have been created.
        // for trigger in triggers {
        //     block_state = self
        //         .ctx
        //         .instance
        //         .trigger_processor
        //         .process_trigger(
        //             &logger,
        //             &runtime_hosts,
        //             &block,
        //             &trigger,
        //             block_state,
        //             &proof_of_indexing,
        //             &causality_region,
        //             &self.inputs.debug_fork,
        //             &self.metrics.subgraph,
        //         )
        //         .await
        //         .map_err(|e| {
        //             // This treats a `PossibleReorg` as an ordinary error which will fail the subgraph.
        //             // This can cause an unnecessary subgraph failure, to fix it we need to figure out a
        //             // way to revert the effect of `create_dynamic_data_sources` so we may return a
        //             // clean context as in b21fa73b-6453-4340-99fb-1a78ec62efb1.
        //             match e {
        //                 MappingError::PossibleReorg(e) | MappingError::Unknown(e) => {
        //                     BlockProcessingError::Unknown(e)
        //                 }
        //             }
        //         })?;
        // }

        //maybe dead lock? sequence ctx inputs state logger metrics 
        let mut self_ctx_mutex = self_ctx_copy.lock().await;
        let self_inputs_mutex = self_inputs_copy.lock().await;     
        let self_metrics_mutex = self_metrics_copy.lock().await;   

        block_state = self_ctx_mutex
            .instance
            .trigger_processor
            .process_trigger(
                &logger,
                &runtime_hosts,
                &block,
                &triggers,
                block_state,
                &proof_of_indexing,
                &causality_region,
                &self_inputs_mutex.debug_fork,
                &self_metrics_mutex.subgraph,
            )
            .await
            .map_err(|e| {
                // This treats a `PossibleReorg` as an ordinary error which will fail the subgraph.
                // This can cause an unnecessary subgraph failure, to fix it we need to figure out a
                // way to revert the effect of `create_dynamic_data_sources` so we may return a
                // clean context as in b21fa73b-6453-4340-99fb-1a78ec62efb1.
                match e {
                    MappingError::PossibleReorg(e) | MappingError::Unknown(e) => {
                        BlockProcessingError::Unknown(e)
                    }
                }
            })?;

    }


    let has_errors = block_state.has_errors();
    
    
    let is_non_fatal_errors_active =

    {
        let self_inputs_mutex = self_inputs_copy.lock().await;  
        let is_non_fatal_errors_active = self_inputs_mutex
        .features
        .contains(&SubgraphFeature::NonFatalErrors);
        is_non_fatal_errors_active
    };






    // Apply entity operations and advance the stream

    // Avoid writing to store if block stream has been canceled
    if block_stream_cancel_handle.is_canceled() {
        return Err(BlockProcessingError::Canceled);
    }

    // if let Some(proof_of_indexing) = proof_of_indexing {
    //     let proof_of_indexing = Arc::try_unwrap(proof_of_indexing).unwrap().into_inner();
    //     update_proof_of_indexing(
    //         proof_of_indexing,
    //         &self.metrics.host.stopwatch,
    //         &self.inputs.deployment.hash,
    //         &mut block_state.entity_cache,
    //     )
    //     .await?;
    // }
    let section = 
    {
        let self_metrics_mutex = self_metrics_copy.lock().await;  
        let section = self_metrics_mutex
        .host
        .stopwatch
        .start_section("as_modifications");

        section
    };




    let ModificationsAndCache {
        modifications: mut mods,
        data_sources,
        entity_lfu_cache: cache,
    } = block_state
        .entity_cache
        .as_modifications()
        .map_err(|e| BlockProcessingError::Unknown(e.into()))?;
    section.end();




    {

        let mut self_state_mutex = self_state_copy.lock().await;  
        // Put the cache back in the state, asserting that the placeholder cache was not used.
        // assert!(self_state_mutex.entity_lfu_cache.is_empty());
        self_state_mutex.entity_lfu_cache = cache;

    }







    if !mods.is_empty() {
        info!(&logger, "Applying {} entity operation(s)", mods.len());
    }

    let err_count = block_state.deterministic_errors.len();
    for (i, e) in block_state.deterministic_errors.iter().enumerate() {
        let message = format!("{:#}", e).replace("\n", "\t");
        error!(&logger, "Subgraph error {}/{}", i + 1, err_count;
            "error" => message,
            "code" => LogCode::SubgraphSyncingFailure
        );
    }




    let self_inputs_mutex = self_inputs_copy.lock().await; 
    let self_metrics_mutex = self_metrics_copy.lock().await; 

    // Transact entity operations into the store and update the
    // subgraph's block stream pointer
    let _section = self_metrics_mutex.host.stopwatch.start_section("transact_block");




    let start = Instant::now();

    let store = &self_inputs_mutex.store; 


    // If a deterministic error has happened, make the PoI to be the only entity that'll be stored.
    if has_errors && !is_non_fatal_errors_active {
        let is_poi_entity =
            |entity_mod: &EntityModification| entity_mod.entity_key().entity_type.is_poi();
        mods.retain(is_poi_entity);
        // Confidence check
        assert!(
            mods.len() == 1,
            "There should be only one PoI EntityModification"
        );
    }

    let BlockState {
        deterministic_errors,
        ..
    } = block_state;

    let first_error = deterministic_errors.first().cloned();


    //debug==========================================================================================
    for x in &mods{
        let y = x.clone();
        println!("{:?}",y.entity_key());
        println!("{:?}",y.entity());
    }

    store
        .transact_block_operations(
            block_ptr,
            firehose_cursor,
            mods,
            &self_metrics_mutex.host.stopwatch,
            data_sources,
            deterministic_errors,
            self_inputs_mutex.manifest_idx_and_name.clone(),
        )
        .await
        .context("Failed to transact block operations")?;

    // For subgraphs with `nonFatalErrors` feature disabled, we consider
    // any error as fatal.
    //
    // So we do an early return to make the subgraph stop processing blocks.
    //
    // In this scenario the only entity that is stored/transacted is the PoI,
    // all of the others are discarded.
    if has_errors && !is_non_fatal_errors_active {
        // Only the first error is reported.
        return Err(BlockProcessingError::Deterministic(first_error.unwrap()));
    }

    let elapsed = start.elapsed().as_secs_f64();
    self_metrics_mutex
        .subgraph
        .block_ops_transaction_duration
        .observe(elapsed);

    // To prevent a buggy pending version from replacing a current version, if errors are
    // present the subgraph will be unassigned.
    if has_errors && !ENV_VARS.disable_fail_fast && !store.is_deployment_synced().await? {
        store
            .unassign_subgraph()
            .map_err(|e| BlockProcessingError::Unknown(e.into()))?;

        // Use `Canceled` to avoiding setting the subgraph health to failed, an error was
        // just transacted so it will be already be set to unhealthy.
        return Err(BlockProcessingError::Canceled);
    }

    match needs_restart {
        true => Ok(Action::Restart),
        false => Ok(Action::Continue),
    }
}

async fn process_triggers<C: Blockchain,T:RuntimeHostBuilder<C>>(
    self_ctx_copy: Arc<Mutex<IndexingContext<C, T>>>,
    self_inputs_copy: Arc<Mutex<IndexingInputs<C>>>,
    self_state_copy: Arc<Mutex<IndexingState>>,
    self_logger_copy: Arc<Mutex<Logger>>,
    self_metrics_copy: Arc<Mutex<RunnerMetrics>>,
    proof_of_indexing: &SharedProofOfIndexing,
    block: &Arc<C::Block>,
    triggers: Vec<C::TriggerData>,
    causality_region: &str,
) -> Result<BlockState<C>, MappingError> {

    let self_inputs_store_clone =
    {
        let self_inputs_mutex = self_inputs_copy.lock().await;
        self_inputs_mutex.store.clone()

    };
    
    let mut block_state = 
    {
        let mut self_state_mutex = self_state_copy.lock().await; 
        let mut block_state = BlockState::new(
            self_inputs_store_clone,
            std::mem::take(&mut self_state_mutex.entity_lfu_cache),
        );
        block_state
    };



    use graph::blockchain::TriggerData;
    // println!("triggers num: {}",triggers.len());
    let start = Instant::now();

    // for trigger in triggers {
    //     block_state = self
    //         .ctx
    //         .instance
    //         .process_trigger(
    //             &self.logger,
    //             block,
    //             &trigger,
    //             block_state,
    //             proof_of_indexing,
    //             causality_region,
    //             &self.inputs.debug_fork,
    //             &self.metrics.subgraph,
    //         )
    //         .await
    //         .map_err(move |mut e| {
    //             let error_context = trigger.error_context();
    //             if !error_context.is_empty() {
    //                 e = e.context(error_context);
    //             }
    //             e.context("failed to process trigger".to_string())
    //         })?;
    // }


    {
    //maybe dead lock? sequence ctx inputs state logger metric 
        let self_ctx_mutex = self_ctx_copy.lock().await;
        let self_inputs_mutex = self_inputs_copy.lock().await;

        let self_logger_mutex = self_logger_copy.lock().await;
        let self_metrics_mutex = self_metrics_copy.lock().await;
        
        block_state = self_ctx_mutex
        .instance
        .process_trigger(
            &self_logger_mutex,
            block,
            &triggers,
            block_state,
            proof_of_indexing,
            causality_region,
            &self_inputs_mutex.debug_fork,
            &self_metrics_mutex.subgraph,
        )
        .await
        .map_err(move |mut e| {
            let error_context = triggers[0].error_context();
            if !error_context.is_empty() {
                e = e.context(error_context);
            }
            e.context("failed to process trigger".to_string())
        })?;


    }
    //todo============================================================================================



    let elapsed = start.elapsed().as_secs_f64();
//=========================================================================================================
//         println!("process_triggers used time: {}",elapsed);



    Ok(block_state)
}

async fn create_dynamic_data_sources<C: Blockchain,T:RuntimeHostBuilder<C>>(
    self_ctx_copy: Arc<Mutex<IndexingContext<C, T>>>,
    self_inputs_copy: Arc<Mutex<IndexingInputs<C>>>,
    self_logger_copy: Arc<Mutex<Logger>>,
    self_metrics_copy: Arc<Mutex<RunnerMetrics>>,
    created_data_sources: Vec<DataSourceTemplateInfo<C>>,
) -> Result<(Vec<C::DataSource>, Vec<Arc<T::Host>>), Error> {
    let mut data_sources = vec![];
    let mut runtime_hosts = vec![];

    //maybe dead lock? sequence ctx inputs state logger metrics 
    let mut self_ctx_mutex = self_ctx_copy.lock().await;
    let self_inputs_mutex = self_inputs_copy.lock().await;

    let self_logger_mutex = self_logger_copy.lock().await;
    let self_metrics_mutex = self_metrics_copy.lock().await;


    for info in created_data_sources {
        // Try to instantiate a data source from the template
        let data_source = C::DataSource::try_from(info)?;

        // Try to create a runtime host for the data source
        let host = self_ctx_mutex.instance.add_dynamic_data_source(
            &self_logger_mutex,
            data_source.clone(),
            self_inputs_mutex.templates.clone(),
            self_metrics_mutex.host.clone(),
        )?;

        match host {
            Some(host) => {
                data_sources.push(data_source);
                runtime_hosts.push(host);
            }
            None => {
                warn!(
                    self_logger_mutex,
                    "no runtime hosted created, there is already a runtime host instantiated for \
                    this data source";
                    "name" => &data_source.name(),
                    "address" => &data_source.address()
                    .map(|address| hex::encode(address))
                    .unwrap_or("none".to_string()),
                )
            }
        }
        
    }

    Ok((data_sources, runtime_hosts))
}

async fn persist_dynamic_data_sources<C: Blockchain,T:RuntimeHostBuilder<C>>(
    self_ctx_copy: Arc<Mutex<IndexingContext<C, T>>>,

    self_logger_copy: Arc<Mutex<Logger>>,

    entity_cache: &mut EntityCache,
    data_sources: Vec<C::DataSource>,
) {


    //maybe dead lock? sequence ctx inputs state logger metrics 
    let mut self_ctx_mutex = self_ctx_copy.lock().await;

    let self_logger_mutex = self_logger_copy.lock().await;


    if !data_sources.is_empty() {
        debug!(
            self_logger_mutex,
            "Creating {} dynamic data source(s)",
            data_sources.len()
        );
    }

    // Add entity operations to the block state in order to persist
    // the dynamic data sources
    for data_source in data_sources.iter() {
        debug!(
            self_logger_mutex,
            "Persisting data_source";
            "name" => &data_source.name(),
            "address" => &data_source.address().map(|address| hex::encode(address)).unwrap_or("none".to_string()),
        );
        entity_cache.add_data_source(data_source);
    }

    // Merge filters from data sources into the block stream builder
    self_ctx_mutex.filter.extend(data_sources.iter());
}




// impl<C, T> SubgraphRunner<C, T>
// where
//     C: Blockchain,
//     T: RuntimeHostBuilder<C>,
// {
async fn handle_stream_event<C: Blockchain,T:RuntimeHostBuilder<C>>(
    self_ctx_copy: Arc<Mutex<IndexingContext<C, T>>>,
    self_inputs_copy: Arc<Mutex<IndexingInputs<C>>>,
    self_state_copy: Arc<Mutex<IndexingState>>,
    self_logger_copy: Arc<Mutex<Logger>>,
    self_metrics_copy: Arc<Mutex<RunnerMetrics>>,
    event: Option<Result<BlockStreamEvent<C>, CancelableError<Error>>>,
    cancel_handle: &CancelHandle,
) -> Result<Action, Error> {
    let action = match event {
        Some(Ok(BlockStreamEvent::ProcessBlock(block, cursor))) => {

            handle_process_block(self_ctx_copy,self_inputs_copy,self_state_copy,self_logger_copy,self_metrics_copy,block, cursor, cancel_handle)
                .await?
        }
        Some(Ok(BlockStreamEvent::Revert(revert_to_ptr, cursor))) => {
            handle_revert(self_ctx_copy,self_inputs_copy,self_state_copy,self_logger_copy,self_metrics_copy,revert_to_ptr, cursor).await?
        }
        // Log and drop the errors from the block_stream
        // The block stream will continue attempting to produce blocks
        Some(Err(e)) => handle_err(self_logger_copy,e, cancel_handle).await?,
        // If the block stream ends, that means that there is no more indexing to do.
        // Typically block streams produce indefinitely, but tests are an example of finite block streams.
        None => Action::Stop,
    };

    Ok(action)
}
// }

enum Action {
    Continue,
    Stop,
    Restart,
}

// #[async_trait]
// trait StreamEventHandler<C: Blockchain> {
//     async fn handle_process_block(
//         &mut self,
//         block: BlockWithTriggers<C>,
//         cursor: FirehoseCursor,
//         cancel_handle: &CancelHandle,
//     ) -> Result<Action, Error>;
//     async fn handle_revert(
//         &mut self,
//         revert_to_ptr: BlockPtr,
//         cursor: FirehoseCursor,
//     ) -> Result<Action, Error>;
//     async fn handle_err(
//         &mut self,
//         err: CancelableError<Error>,
//         cancel_handle: &CancelHandle,
//     ) -> Result<Action, Error>;
// }
//         // input,
//         // metrics,
// #[async_trait]
// impl<C, T> StreamEventHandler<C> for SubgraphRunner<C, T>
// where
//     C: Blockchain,
//     T: RuntimeHostBuilder<C>,
// {
async fn handle_process_block<C: Blockchain,T:RuntimeHostBuilder<C>>(
    self_ctx_copy: Arc<Mutex<IndexingContext<C, T>>>,
    self_inputs_copy: Arc<Mutex<IndexingInputs<C>>>,
    self_state_copy: Arc<Mutex<IndexingState>>,
    self_logger_copy: Arc<Mutex<Logger>>,
    self_metrics_copy: Arc<Mutex<RunnerMetrics>>,
    block: BlockWithTriggers<C>,
    cursor: FirehoseCursor,
    cancel_handle: &CancelHandle,
) -> Result<Action, Error> {
    let block_ptr = block.ptr();

    {
        let self_metrics_mutex = self_metrics_copy.lock().await;
        self_metrics_mutex
            .stream
            .deployment_head
            .set(block_ptr.number as f64);

        if block.trigger_count() > 0 {
            self_metrics_mutex
                .subgraph
                .block_trigger_count
                .observe(block.trigger_count() as f64);
        }
    }


    {
        //maybe dead lock? sequence ctx inputs state logger metrics 

        let self_inputs_mutex = self_inputs_copy.lock().await;
        let mut self_state_mutex = self_state_copy.lock().await;
        if block.trigger_count() == 0
            && self_state_mutex.skip_ptr_updates_timer.elapsed() <= SKIP_PTR_UPDATES_THRESHOLD
            && !self_state_mutex.synced
            && !close_to_chain_head(
                &block_ptr,
                self_inputs_mutex.chain.chain_store().cached_head_ptr().await?,
                // The "skip ptr updates timer" is ignored when a subgraph is at most 1000 blocks
                // behind the chain head.
                1000,
            )
        {
            return Ok(Action::Continue);
        } else {
            self_state_mutex.skip_ptr_updates_timer = Instant::now();
        }
    }
    let start = Instant::now();
    
    let self_inputs_copy_copy = self_inputs_copy.clone();
    let self_ctx_copy_copy = self_ctx_copy.clone();
    let self_logger_copy_copy = self_logger_copy.clone();
    let self_metrics_copy_copy = self_metrics_copy.clone();
    let self_state_copy_copy = self_state_copy.clone();

    let res = process_block(self_ctx_copy_copy,self_inputs_copy_copy,self_state_copy_copy,self_logger_copy_copy,self_metrics_copy_copy,&cancel_handle, block, cursor).await;


    let elapsed = start.elapsed().as_secs_f64();
//=========================================================================================================
    println!("block used time: {}",elapsed);

    {

        let self_metrics_mutex = self_metrics_copy.lock().await;

        self_metrics_mutex
        .subgraph
        .block_processing_duration
        .observe(elapsed);


    }


    {

        //maybe dead lock? sequence ctx inputs state logger metrics 
        let self_ctx_mutex = self_ctx_copy.lock().await;
        let self_inputs_mutex = self_inputs_copy.lock().await;
        let mut self_state_mutex = self_state_copy.lock().await;
        let self_logger_mutex = self_logger_copy.lock().await;
        let self_metrics_mutex = self_metrics_copy.lock().await;


        match res {
            Ok(action) => {
                // Once synced, no need to try to update the status again.
                if !self_state_mutex.synced
                    && close_to_chain_head(
                        &block_ptr,
                        self_inputs_mutex.chain.chain_store().cached_head_ptr().await?,
                        // We consider a subgraph synced when it's at most 1 block behind the
                        // chain head.
                        1,
                    )
                {
                    // Updating the sync status is an one way operation.
                    // This state change exists: not synced -> synced
                    // This state change does NOT: synced -> not synced
                    self_inputs_mutex.store.deployment_synced()?;

                    // Stop trying to update the sync status.
                    self_state_mutex.synced = true;

                    // Stop recording time-to-sync metrics.
                    self_metrics_mutex.stream.stopwatch.disable();
                }

                // Keep trying to unfail subgraph for everytime it advances block(s) until it's
                // health is not Failed anymore.
                if self_state_mutex.should_try_unfail_non_deterministic {
                    // If the deployment head advanced, we can unfail
                    // the non-deterministic error (if there's any).
                    let outcome = self_inputs_mutex
                        .store
                        .unfail_non_deterministic_error(&block_ptr)?;

                    if let UnfailOutcome::Unfailed = outcome {
                        // Stop trying to unfail.
                        self_state_mutex.should_try_unfail_non_deterministic = false;
                        self_metrics_mutex.stream.deployment_failed.set(0.0);
                        self_state_mutex.backoff.reset();
                    }
                }

                if let Some(stop_block) = &self_inputs_mutex.stop_block {
                    if block_ptr.number >= *stop_block {
                        info!(self_logger_mutex, "stop block reached for subgraph");
                        return Ok(Action::Stop);
                    }
                }

                if matches!(action, Action::Restart) {
                    // Cancel the stream for real
                    self_ctx_mutex
                        .instances
                        .write()
                        .unwrap()
                        .remove(&self_inputs_mutex.deployment.id);

                    // And restart the subgraph
                    return Ok(Action::Restart);
                }

                return Ok(Action::Continue);
            }
            Err(BlockProcessingError::Canceled) => {
                debug!(self_logger_mutex, "Subgraph block stream shut down cleanly");
                return Ok(Action::Stop);
            }

            // Handle unexpected stream errors by marking the subgraph as failed.
            Err(e) => {
                // Clear entity cache when a subgraph fails.
                //
                // This is done to be safe and sure that there's no state that's
                // out of sync from the database.
                //
                // Without it, POI changes on failure would be kept in the entity cache
                // and be transacted incorrectly in the next run.
                self_state_mutex.entity_lfu_cache = LfuCache::new();

                self_metrics_mutex.stream.deployment_failed.set(1.0);

                let message = format!("{:#}", e).replace("\n", "\t");
                let err = anyhow!("{}, code: {}", message, LogCode::SubgraphSyncingFailure);
                let deterministic = e.is_deterministic();

                let error = SubgraphError {
                    subgraph_id: self_inputs_mutex.deployment.hash.clone(),
                    message,
                    block_ptr: Some(block_ptr),
                    handler: None,
                    deterministic,
                };

                match deterministic {
                    true => {
                        // Fail subgraph:
                        // - Change status/health.
                        // - Save the error to the database.
                        self_inputs_mutex
                            .store
                            .fail_subgraph(error)
                            .await
                            .context("Failed to set subgraph status to `failed`")?;

                        return Err(err);
                    }
                    false => {
                        // Shouldn't fail subgraph if it's already failed for non-deterministic
                        // reasons.
                        //
                        // If we don't do this check we would keep adding the same error to the
                        // database.
                        let should_fail_subgraph =
                            self_inputs_mutex.store.health().await? != SubgraphHealth::Failed;

                        if should_fail_subgraph {
                            // Fail subgraph:
                            // - Change status/health.
                            // - Save the error to the database.
                            self_inputs_mutex
                                .store
                                .fail_subgraph(error)
                                .await
                                .context("Failed to set subgraph status to `failed`")?;
                        }

                        // Retry logic below:

                        // Cancel the stream for real.
                        self_ctx_mutex
                            .instances
                            .write()
                            .unwrap()
                            .remove(&self_inputs_mutex.deployment.id);

                        let message = format!("{:#}", e).replace("\n", "\t");
                        error!(self_logger_mutex, "Subgraph failed with non-deterministic error: {}", message;
                            "attempt" => self_state_mutex.backoff.attempt,
                            "retry_delay_s" => self_state_mutex.backoff.delay().as_secs());

                        // Sleep before restarting.
                        self_state_mutex.backoff.sleep_async().await;

                        self_state_mutex.should_try_unfail_non_deterministic = true;

                        // And restart the subgraph.
                        return Ok(Action::Restart);
                    }
                }
            }
        }
    }
}

async fn handle_revert<C: Blockchain,T:RuntimeHostBuilder<C>>(
    self_ctx_copy: Arc<Mutex<IndexingContext<C, T>>>,
    self_inputs_copy: Arc<Mutex<IndexingInputs<C>>>,
    self_state_copy: Arc<Mutex<IndexingState>>,
    self_logger_copy: Arc<Mutex<Logger>>,
    self_metrics_copy: Arc<Mutex<RunnerMetrics>>,
    revert_to_ptr: BlockPtr,
    cursor: FirehoseCursor,
) -> Result<Action, Error> {
    // Current deployment head in the database / WritableAgent Mutex cache.
    //
    // Safe unwrap because in a Revert event we're sure the subgraph has
    // advanced at least once.


    let subgraph_ptr = 
    {
        //maybe dead lock? sequence ctx inputs state logger metrics 

        let self_inputs_mutex = self_inputs_copy.lock().await; 
        let self_logger_mutex = self_logger_copy.lock().await; 



        let subgraph_ptr = self_inputs_mutex.store.block_ptr().unwrap();

        if revert_to_ptr.number >= subgraph_ptr.number {
            info!(&self_logger_mutex, "Block to revert is higher than subgraph pointer, nothing to do"; "subgraph_ptr" => &subgraph_ptr, "revert_to_ptr" => &revert_to_ptr);
            return Ok(Action::Continue);
        }

        info!(&self_logger_mutex, "Reverting block to get back to main chain"; "subgraph_ptr" => &subgraph_ptr, "revert_to_ptr" => &revert_to_ptr);




        if let Err(e) = self_inputs_mutex
        .store
        .revert_block_operations(revert_to_ptr, cursor)
        .await
        {
            error!(&self_logger_mutex, "Could not revert block. Retrying"; "error" => %e);

            // Exit inner block stream consumption loop and go up to loop that restarts subgraph
            return Ok(Action::Restart);
        }

        subgraph_ptr

    };

    {

        let self_metrics_mutex = self_metrics_copy.lock().await; 

        self_metrics_mutex
            .stream
            .reverted_blocks
            .set(subgraph_ptr.number as f64);
        self_metrics_mutex
            .stream
            .deployment_head
            .set(subgraph_ptr.number as f64);
            
    }






    // Revert the in-memory state:
    // - Remove hosts for reverted dynamic data sources.
    // - Clear the entity cache.
    //
    // Note that we do not currently revert the filters, which means the filters
    // will be broader than necessary. This is not ideal for performance, but is not
    // incorrect since we will discard triggers that match the filters but do not
    // match any data sources.

    {

        let mut self_ctx_mutex = self_ctx_copy.lock().await; 
        
        self_ctx_mutex.instance.revert_data_sources(subgraph_ptr.number);
        
    }




    {

        let mut self_state_mutex = self_state_copy.lock().await; 
        self_state_mutex.entity_lfu_cache = LfuCache::new();
        
    }





    Ok(Action::Continue)
}

async fn handle_err(
    self_logger_copy: Arc<Mutex<Logger>>,
    err: CancelableError<Error>,
    cancel_handle: &CancelHandle,
) -> Result<Action, Error> {
    let self_logger_mutex = self_logger_copy.lock().await; 
    if cancel_handle.is_canceled() {
        debug!(&self_logger_mutex, "Subgraph block stream shut down cleanly");
        return Ok(Action::Stop);
    }

    debug!(
        &self_logger_mutex,
        "Block stream produced a non-fatal error";
        "error" => format!("{}", err),
    );

    Ok(Action::Continue)
}
// }

/// Transform the proof of indexing changes into entity updates that will be
/// inserted when as_modifications is called.
async fn update_proof_of_indexing(
    proof_of_indexing: ProofOfIndexing,
    stopwatch: &StopwatchMetrics,
    deployment_id: &DeploymentHash,
    entity_cache: &mut EntityCache,
) -> Result<(), Error> {
    let _section_guard = stopwatch.start_section("update_proof_of_indexing");

    let mut proof_of_indexing = proof_of_indexing.take();

    for (causality_region, stream) in proof_of_indexing.drain() {
        // Create the special POI entity key specific to this causality_region
        let entity_key = EntityKey {
            subgraph_id: deployment_id.clone(),
            entity_type: POI_OBJECT.to_owned(),
            entity_id: causality_region,
        };

        // Grab the current digest attribute on this entity
        let prev_poi =
            entity_cache
                .get(&entity_key)
                .map_err(Error::from)?
                .map(|entity| match entity.get("digest") {
                    Some(Value::Bytes(b)) => b.clone(),
                    _ => panic!("Expected POI entity to have a digest and for it to be bytes"),
                });

        // Finish the POI stream, getting the new POI value.
        let updated_proof_of_indexing = stream.pause(prev_poi.as_deref());
        let updated_proof_of_indexing: Bytes = (&updated_proof_of_indexing[..]).into();

        // Put this onto an entity with the same digest attribute
        // that was expected before when reading.
        let new_poi_entity = entity! {
            id: entity_key.entity_id.clone(),
            digest: updated_proof_of_indexing,
        };

        entity_cache.set(entity_key, new_poi_entity)?;
    }

    Ok(())
}

/// Checks if the Deployment BlockPtr is at least X blocks behind to the chain head.
fn close_to_chain_head(
    deployment_head_ptr: &BlockPtr,
    chain_head_ptr: Option<BlockPtr>,
    n: BlockNumber,
) -> bool {
    matches!((deployment_head_ptr, &chain_head_ptr), (b1, Some(b2)) if b1.number >= (b2.number - n))
}

#[test]
fn test_close_to_chain_head() {
    let offset = 1;

    let block_0 = BlockPtr::try_from((
        "bd34884280958002c51d3f7b5f853e6febeba33de0f40d15b0363006533c924f",
        0,
    ))
    .unwrap();
    let block_1 = BlockPtr::try_from((
        "8511fa04b64657581e3f00e14543c1d522d5d7e771b54aa3060b662ade47da13",
        1,
    ))
    .unwrap();
    let block_2 = BlockPtr::try_from((
        "b98fb783b49de5652097a989414c767824dff7e7fd765a63b493772511db81c1",
        2,
    ))
    .unwrap();

    assert!(!close_to_chain_head(&block_0, None, offset));
    assert!(!close_to_chain_head(&block_2, None, offset));

    assert!(!close_to_chain_head(
        &block_0,
        Some(block_2.clone()),
        offset
    ));

    assert!(close_to_chain_head(&block_1, Some(block_2.clone()), offset));
    assert!(close_to_chain_head(&block_2, Some(block_2.clone()), offset));
}
