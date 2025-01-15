use anyhow::Result;
use log::{info, warn};
use serde_yaml::Value;
use std::time::Duration;
use vector::config::{init_log_schema, init_telemetry, Config, ConfigBuilder};

pub fn build_config(yaml_config: &mut Value) -> Result<Config> {
    build_config_builder(build_yaml_config(yaml_config)?)
}

pub fn build_yaml_config(yaml_config: &mut Value) -> Result<ConfigBuilder> {
    yaml_config.apply_merge()?;
    let config_builder = serde_yaml::from_value::<ConfigBuilder>(yaml_config.clone())?;
    Ok(config_builder)
}

pub fn build_config_builder(config_builder: ConfigBuilder) -> Result<Config> {
    let (mut config, build_warnings) = config_builder
        .build_with_warnings()
        .map_err(|e| anyhow::anyhow!(e.join(",")))?;

    // can not validate config here because validation functions is not public visible
    for warning in build_warnings {
        warn!("{}", warning);
    }
    // deny_if_set: false means allow rebuild config, and don't panic during reset
    init_log_schema(config.global.log_schema.clone(), false);
    init_telemetry(config.global.telemetry.clone(), false);

    if !config.healthchecks.enabled {
        info!("healthchecks disabled");
    } else {
        config
            .healthchecks
            .set_require_healthy(config.healthchecks.enabled);
    }
    config.graceful_shutdown_duration = Some(Duration::from_secs(60u64));

    Ok(config)
}

mod tests {
    #[test]
    fn test_config_builder() {
        let string_config = "
sources:
  demo_logs:
    type: demo_logs
    format: json
    interval: 1
sinks:
  print:
    type: console
    inputs:
    - demo_logs
    encoding:
      codec: json
";
        let mut yaml_config = serde_yaml::from_str::<Value>(string_config).unwrap();
        match build_config_yaml(&mut yaml_config) {
            Ok(config) => {
                assert_eq!(
                    config.sources().filter(|x| x.0.id() == "demo_logs").count(),
                    1
                );
                assert_eq!(config.sinks().filter(|x| x.0.id() == "print").count(), 1);
                let sinks = config.sinks().find(|x| x.0.id() == "print").unwrap().1;
                assert_eq!(sinks.inputs.len(), 1);
            }
            Err(e) => {
                panic!("Failed to build vector config: {}", e);
            }
        }
    }
}
