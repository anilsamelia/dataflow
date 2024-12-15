package com.dataflow.dataanalyticsfoundation.sftp;

import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * This class is a user-defined where its process to make connect and disconnect sftp connection.
 * 
 * @author Barkat Dhillon
 * @version 1.0
 * @since 2022-02-08
 */

public class SftpConnection {

    private static final Logger LOG = LoggerFactory.getLogger(SftpConnection.class);

    private String host;
    private String username;
    private String password;
    private String inputFolder;

    private Session session = null;

    public SftpConnection(String host, String username, String password, String inputFolder) {
        this.host = host;
        this.username = username;
        this.password = password;
        this.inputFolder = inputFolder;
    }
    
	/**
	 * getChannel method is used to built connection with server using sftp with
	 * required parameter like session,password and other configuration.
	 * 
	 */

    public ChannelSftp getChannel() {
        JSch jSch = new JSch();
        ChannelSftp channelSftp = null;
        try {
            session = jSch.getSession(username, host, 22);
            Properties config = new Properties();
            config.put("StrictHostKeyChecking", "no");
            session.setPassword(password);
            session.setConfig(config);
            LOG.debug("Connecting SSH to " + host + " - Please wait for few seconds... ");
            session.connect();
            LOG.info("Connected to: "+host);

            channelSftp = (ChannelSftp) session.openChannel("sftp");
            LOG.info("Connecting SFTP channel...");
            channelSftp.connect();
            LOG.info("Connected to SFTP channel");

            if(inputFolder != null && !inputFolder.trim().equals("")) {
                LOG.info("Changing SFTP Channel Directory to: "+inputFolder);
                channelSftp.cd(inputFolder);
            }

        } catch (Exception ex) {
            LOG.error("Error occurred while connecting to server or SFTP Channel: "+ex.getMessage(), ex);
        }
        return channelSftp;
    }

	/**
	 * disconnect method is used to disconnect server or sftp channel
	 * 
	 * @param ChannelSftp    channelSftp
	 * 
	 */

    
    public void disconnect(ChannelSftp channelSftp) {
        LOG.info("Disconnecting...");
        if (channelSftp != null) {
            LOG.info("Disconnecting SFTP Channel...");
            channelSftp.disconnect();
        }
        if (session != null) {
            LOG.info("Disconnecting Server session...");
            session.disconnect();
        }
    }
}
