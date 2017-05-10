package org.corefine.mysqlsync.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties("desc")
public class DescConfig extends DbConfig {}
