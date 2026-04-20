package com.loomq.config;

import org.aeonbits.owner.ConfigFactory;

import java.util.Properties;

/**
 * 主配置类
 */
public class LoomqConfig {
    private static volatile LoomqConfig instance;

    private final ServerConfig serverConfig;
    private final WalConfig walConfig;
    private final SchedulerConfig schedulerConfig;
    private final DispatcherConfig dispatcherConfig;
    private final RetryConfig retryConfig;
    private final RecoveryConfig recoveryConfig;

    private LoomqConfig() {
        this.serverConfig = ConfigFactory.create(ServerConfig.class);
        this.walConfig = ConfigFactory.create(WalConfig.class);
        this.schedulerConfig = ConfigFactory.create(SchedulerConfig.class);
        this.dispatcherConfig = ConfigFactory.create(DispatcherConfig.class);
        this.retryConfig = ConfigFactory.create(RetryConfig.class);
        this.recoveryConfig = ConfigFactory.create(RecoveryConfig.class);
    }

    public static LoomqConfig getInstance() {
        if (instance == null) {
            synchronized (LoomqConfig.class) {
                if (instance == null) {
                    instance = new LoomqConfig();
                }
            }
        }
        return instance;
    }

    public static void reload(Properties properties) {
        synchronized (LoomqConfig.class) {
            if (properties != null && !properties.isEmpty()) {
                instance = new LoomqConfig(properties);
            } else {
                instance = new LoomqConfig();
            }
        }
    }

    private LoomqConfig(Properties properties) {
        this.serverConfig = ConfigFactory.create(ServerConfig.class, properties);
        this.walConfig = ConfigFactory.create(WalConfig.class, properties);
        this.schedulerConfig = ConfigFactory.create(SchedulerConfig.class, properties);
        this.dispatcherConfig = ConfigFactory.create(DispatcherConfig.class, properties);
        this.retryConfig = ConfigFactory.create(RetryConfig.class, properties);
        this.recoveryConfig = ConfigFactory.create(RecoveryConfig.class, properties);
    }

    public ServerConfig getServerConfig() {
        return serverConfig;
    }

    public WalConfig getWalConfig() {
        return walConfig;
    }

    public SchedulerConfig getSchedulerConfig() {
        return schedulerConfig;
    }

    public DispatcherConfig getDispatcherConfig() {
        return dispatcherConfig;
    }

    public RetryConfig getRetryConfig() {
        return retryConfig;
    }

    public RecoveryConfig getRecoveryConfig() {
        return recoveryConfig;
    }
}
