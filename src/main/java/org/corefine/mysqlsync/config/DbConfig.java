package org.corefine.mysqlsync.config;

public class DbConfig {
	private String host;
	private String username;
	private String password;
	private Integer port;
	public String getHost() {
		return host;
	}
	public void setHost(String host) {
		this.host = host;
	}
	public String getUsername() {
		return username;
	}
	public void setUsername(String username) {
		this.username = username;
	}
	public String getPassword() {
		return password;
	}
	public void setPassword(String password) {
		this.password = password;
	}
	public Integer getPort() {
		return port;
	}
	public void setPort(Integer port) {
		this.port = port;
	}
	@Override
	public String toString() {
		return "DbConfig [host=" + host + ", username=" + username + ", password=" + password + ", port=" + port + "]";
	}
}
