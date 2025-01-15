use crate::vector_app::VectorApp;
use crate::vector_config::build_yaml_config;

use log::{debug, error, info, warn};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Once;
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use tokio::runtime::Runtime;
use tokio::sync::broadcast;
use tokio_stream::wrappers::UnboundedReceiverStream;
use vector::signal::{SignalRx, SignalTo, SignalTx};
use vector::{app, cli::LogFormat, metrics};

pub struct VectorComponent {
    pub app: Arc<VectorApp>,
    enabled: Arc<AtomicBool>,
    runtime: Arc<Runtime>,
    running: Arc<AtomicBool>,
    signal_tx: SignalTx,
    signal_rx: Arc<Mutex<SignalRx>>,
    thread_handle: Option<JoinHandle<()>>,
}

static INIT: Once = Once::new();

impl VectorComponent {
    const NAME: &str = "vector";
    const VECTOR_LOG_LEVEL: &str = "error";
    const VECTOR_LOG_FORMAT: LogFormat = LogFormat::Text;
    const VECTOR_LOG_RATE_LIMIT: u64 = 10;

    fn build_once() {
        INIT.call_once(|| {
            // prepare vector app components
            // https://github.com/vectordotdev/vector/blob/v0.43.1/src/app.rs#L192
            openssl_probe::init_ssl_cert_env_vars();
            // would cause panic if execute twice
            match metrics::init_global() {
                Ok(()) => (),
                Err(e) => {
                    error!("vector inner metrics init failed: {}", e);
                }
            }
            // when vector running as component, will log to stdout by default when config log capture
            // if will lead to log flushing in console or log file, which is not our expected
            // meanwhile, to avoid log circle (log->vector->log->vector...), should not open info+ level(info/debug/trace)
            app::init_logging(
                false,
                Self::VECTOR_LOG_FORMAT,
                Self::VECTOR_LOG_LEVEL,
                Self::VECTOR_LOG_RATE_LIMIT,
            );
        });
    }

    pub fn new(enabled: bool, yaml_config: serde_yaml::Value, runtime: Arc<Runtime>) -> Self {
        if enabled {
            Self::build_once();
        }

        let (tx, rx): (SignalTx, SignalRx) = broadcast::channel(128);
        Self {
            app: Arc::new(VectorApp::new(yaml_config)),
            enabled: Arc::new(AtomicBool::new(enabled)),
            runtime,
            running: Arc::new(AtomicBool::new(false)),
            signal_tx: tx,
            signal_rx: Arc::new(Mutex::new(rx)),
            thread_handle: None,
        }
    }

    pub fn start(&mut self) {
        if !self.enabled.load(Ordering::Relaxed) {
            debug!("{} not enabled", Self::NAME);
            return;
        }
        if self.running.swap(true, Ordering::Relaxed) {
            warn!("{} already running", Self::NAME);
            return;
        }
        let (app, running, runtime, signal_rx) = (
            self.app.clone(),
            self.running.clone(),
            self.runtime.clone(),
            self.signal_rx.clone(),
        );

        self.thread_handle = Some(
            thread::Builder::new()
                .name("vector-worker".to_string())
                .spawn(move || {
                    runtime.block_on(Self::run_app(app, running, signal_rx));
                })
                .unwrap(),
        );
    }

    async fn run_app(
        app: Arc<VectorApp>,
        running: Arc<AtomicBool>,
        signal_rx: Arc<Mutex<SignalRx>>,
    ) {
        let mut signal_rx = signal_rx.lock().unwrap();
        match app.start().await {
            Ok((topology_controller, graceful_crash_receiver)) => {
                info!("{} start success", Self::NAME);
                running.store(true, Ordering::Relaxed);
                let mut graceful_crash = UnboundedReceiverStream::new(graceful_crash_receiver);
                match VectorApp::watch_signals(
                    topology_controller,
                    &mut signal_rx,
                    &mut graceful_crash,
                )
                .await
                {
                    SignalTo::Shutdown(Some(e)) => {
                        info!("{} shutdown error: {}", Self::NAME, e);
                        running.store(false, Ordering::Relaxed);
                    }
                    // signalto::quit send by self, ignore it.
                    _ => {}
                }
            }
            Err(e) => {
                // if app start failed, would not listen signals, don't try to send
                error!("{} start failed: {}", Self::NAME, e);
                running.store(false, Ordering::Relaxed);
            }
        }
    }

    pub fn on_config_change(&mut self, enabled: bool, yaml_config: serde_yaml::Value) {
        if !enabled {
            if self.enabled.swap(enabled, Ordering::Relaxed) {
                // enabled => disable: stop running
                Self::stop(self.running.clone(), self.signal_tx.clone());
            }
        } else {
            if !self.enabled.swap(enabled, Ordering::Relaxed) {
                // disable => enable
                Self::build_once();
            }

            if !self.running.swap(true, Ordering::Relaxed) {
                // disable => enable: start running, should re-new config
                let new_app = Arc::new(VectorApp::new(yaml_config.clone()));
                self.app = new_app;
                let (app, running, runtime, signal_rx) = (
                    self.app.clone(),
                    self.running.clone(),
                    self.runtime.clone(),
                    self.signal_rx.clone(),
                );

                self.thread_handle = Some(
                    thread::Builder::new()
                        .name("vector-worker".to_string())
                        .spawn(move || {
                            runtime.block_on(Self::run_app(app, running, signal_rx));
                        })
                        .unwrap(),
                );
            } else {
                // enable => enable: reload config
                let config_builder =
                    build_yaml_config(&mut yaml_config.clone()).unwrap_or_default();
                let _ = self
                    .signal_tx
                    .send(SignalTo::ReloadFromConfigBuilder(config_builder))
                    .map_err(|e| {
                        error!("{} config change failed: {}", Self::NAME, e);
                    });
            }
        }
    }

    fn stop(running: Arc<AtomicBool>, signal_tx: SignalTx) {
        if !running.swap(false, Ordering::Relaxed) {
            warn!("{} already stopped, skip", Self::NAME);
            return;
        }
        match signal_tx.send(SignalTo::Shutdown(None)) {
            Ok(_) => {
                info!("{} stopped", Self::NAME);
            }
            Err(e) => error!("{} stop get error: {}", Self::NAME, e),
        }
    }

    pub fn notify_stop(&mut self) -> Option<JoinHandle<()>> {
        Self::stop(self.running.clone(), self.signal_tx.clone());
        return self.thread_handle.take();
    }
}
