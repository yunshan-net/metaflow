use anyhow::Result;
use futures::StreamExt;
use log::{debug, error, info};
use tokio_stream::wrappers::UnboundedReceiverStream;
use vector::{
    config::ConfigBuilder,
    extra_context::ExtraContext,
    signal::{ShutdownError, SignalRx, SignalTo},
    topology::{
        ReloadOutcome, RunningTopology, SharedTopologyController, ShutdownErrorReceiver,
        TopologyController,
    },
};

use crate::vector_config::{build_config, build_config_builder};

#[derive(Debug, Clone)]
pub struct VectorApp {
    yaml_config: serde_yaml::Value,
}

impl VectorApp {
    pub fn new(yaml_config: serde_yaml::Value) -> Self {
        Self { yaml_config }
    }

    pub async fn start(&self) -> Result<(SharedTopologyController, ShutdownErrorReceiver)> {
        let mut yaml_config = self.yaml_config.clone();
        let config = build_config(&mut yaml_config)?;
        let require_healthy = config.healthchecks.require_healthy;

        // when build config/components error, graceful_crash_receiver receive messages
        let (topology, graceful_crash_receiver) =
            RunningTopology::start_init_validated(config, ExtraContext::default())
                .await
                .ok_or(anyhow::anyhow!("failed to start vector app"))?;

        // vector api server could not start due to private start up functions
        let topology_controller = SharedTopologyController::new(TopologyController {
            topology,
            config_paths: vec![], // not useful, load config directy from deepflow-agent
            require_healthy: Some(require_healthy),
            extra_context: ExtraContext::default(),
        });

        Ok((topology_controller, graceful_crash_receiver))
    }

    pub(crate) async fn rebuild(
        topology_controller: SharedTopologyController,
        config_builder: ConfigBuilder,
    ) -> Result<()> {
        let mut tc = topology_controller.lock().await;
        match tc
            .reload(build_config_builder(config_builder.clone())?)
            .await
        {
            ReloadOutcome::FatalError(e) => {
                return Err(anyhow::anyhow!(e));
            }
            _ => (),
        }
        Ok(())
    }

    pub(crate) async fn watch_signals(
        topology_controller: SharedTopologyController,
        signal_rx: &mut SignalRx,
        graceful_crash_receiver: &mut UnboundedReceiverStream<ShutdownError>,
    ) -> SignalTo {
        let signal = loop {
            let has_sources = !topology_controller
                .lock()
                .await
                .topology
                .config()
                .is_empty();

            tokio::select! {
                signal = signal_rx.recv() => {
                    match signal {
                        Ok(SignalTo::ReloadFromConfigBuilder(config_builder)) => {
                            match Self::rebuild(topology_controller.clone(), config_builder.clone()).await {
                                Ok(_)=> {
                                    info!("rebuild config topology");
                                    continue
                                },
                                Err(e) => {
                                    error!("failed to reload topology: {:?}", e);
                                    break SignalTo::Shutdown(Some(ShutdownError::ReloadFailedToRestore));
                                }
                            }
                        },
                        Ok(signal) => {
                            // send from vector_component::stop()
                            info!("received signal: {:?}", signal);
                            break SignalTo::Quit;
                        },
                        Err(e) => {
                            error!("signal receiver error: {:?}", e);
                            break SignalTo::Shutdown(None);
                        }
                    }
                },
                Some(error) = graceful_crash_receiver.next() => {
                    // send by vector inner status when build config failed
                    debug!("graceful crash: {:?}", error);
                    break SignalTo::Shutdown(None);
                },
                _ = TopologyController::sources_finished(topology_controller.clone()), if has_sources => {
                    info!("all sources have finished");
                    break SignalTo::Shutdown(None);
                }
            }
        };

        let topology_controller = topology_controller
            .try_into_inner()
            .expect("fail to get topology controller")
            .into_inner();

        match signal {
            SignalTo::Shutdown(_) | SignalTo::Quit => {
                tokio::select! {
                    _ = topology_controller.stop() => {
                        info!("vector topology stopped");
                    },
                    e = signal_rx.recv() => {
                        info!("vector app stopped signal: {:?}", e);
                    },
                }
            }
            _ => unreachable!(),
        };
        signal
    }
}
