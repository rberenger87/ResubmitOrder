package com.amdocs.rollback;

import java.io.Serializable;
import java.util.Hashtable;

import javax.jms.DeliveryMode;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.ObjectMessage;
import javax.jms.Queue;
import javax.jms.QueueConnection;
import javax.jms.QueueConnectionFactory;
import javax.jms.QueueReceiver;
import javax.jms.QueueSender;
import javax.jms.QueueSession;
import javax.jms.TextMessage;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;

import com.att.bbnms.cim.logging.Logger;
import com.att.bbnms.cim.logging.LoggerFactory;

/**
 * Class used to create connections to a JMS/Q
 * 
 * <p>
 * NOTICE NOT FOR USE OR DISCLOSURE OUTSIDE THE BELLSOUTH COMPANIES <br>
 * Copyright 2000. BellSouth Telecommunications. All rights reserved. IPETET Application <br>
 * </p>
 * 
 * @author Chandra Cinthala
 */
public class JmsQueue
{

   private static final Logger LOGGER = LoggerFactory
       .getLoggerWithHostNameAppended(JmsQueue.class);

   protected String jndiURL;

   protected String queueFactoryName;

   protected String queueName;

   protected String username;

   protected String password;

   protected boolean transacted;

   protected InitialContext initialContext;

   protected QueueConnectionFactory factory;

   protected QueueConnection connection;

   protected QueueSession session;

   protected Queue queue;

   protected QueueSender sender;

   protected QueueReceiver receiver;

   protected boolean connected;

   protected int sessionAcknowledge;

   /**
    * Constructor
    */
   public JmsQueue()
   {
      // empty
   }

   /**
    * Constructor to specify connection parameters
    */
   public JmsQueue(String jndiURL, String queueFactoryName, String queueName,
       String username, String password)
   {
      this.jndiURL = jndiURL;
      this.queueFactoryName = queueFactoryName;
      this.queueName = queueName;
      this.username = username;
      this.password = password;
   }

   public JmsQueue(String queueFactoryName, String queueName, String username,
       String password)
   {
      this.queueFactoryName = queueFactoryName;
      this.queueName = queueName;
      this.username = username;
      this.password = password;
   }

   public JmsQueue(String queueFactoryName, String queueName)
   {
      this.queueFactoryName = queueFactoryName;
      this.queueName = queueName;
   }

   /**
    * Initializes the connection given the connection parameters.
    */
   public void initialize() throws JMSException, NamingException
   {
      if (connected)
         return;

      initInitialContext();
      initializeFactory();
      initializeConnection();
      initializeSession();
      initializeQueue();

      connected = true;
   }

   protected void initInitialContext() throws NamingException, JMSException
   {

      Hashtable<String, String> jndiEnv = null;

      if (jndiURL != null)
      {
         jndiEnv = new Hashtable<String, String>();
         jndiEnv.put(Context.INITIAL_CONTEXT_FACTORY,
            "weblogic.jndi.WLInitialContextFactory");
         jndiEnv.put(Context.PROVIDER_URL, jndiURL);
      }
      initialContext = new InitialContext(jndiEnv);
   }

   protected void initializeFactory() throws NamingException
   {
      // Creates a factory to the desired machine/port/channel
      factory = (QueueConnectionFactory) initialContext
         .lookup(queueFactoryName);
   }

   protected void initializeConnection() throws JMSException
   {
      if (username == null && password == null)
         connection = factory.createQueueConnection();
      else
         connection = factory.createQueueConnection(username, password);
   }

   protected void initializeSession() throws JMSException
   {
      session = connection.createQueueSession(transacted, sessionAcknowledge);
   }

   protected void initializeQueue() throws NamingException
   {
      queue = (Queue) initialContext.lookup(queueName);
   }

   protected void initializeSender() throws JMSException
   {
      sender = session.createSender(queue);
   }

   protected void initializeReceiver() throws JMSException
   {
      receiver = session.createReceiver(queue);

      // Starts connection, which enables receipt of messages
      connection.start();
   }

   protected void initializeReceiver(String msgSelector) throws JMSException
   {
      receiver = session.createReceiver(queue, msgSelector);

      // Starts connection, which enables receipt of messages
      connection.start();
   }

   public QueueReceiver getReceiver() throws JMSException, NamingException
   {
      // Creates connect as needed
      if (!connected)
      {
         initialize();
      }

      // Creates sender as needed
      if (receiver == null)
      {
         initializeReceiver();
      }

      return receiver;
   }

   public QueueReceiver getReceiver(String msgSelector) throws JMSException,
       NamingException
   {
      // Creates connect as needed
      if (!connected)
      {
         initialize();
      }

      // Creates sender as needed
      if (receiver == null)
      {
         initializeReceiver(msgSelector);
      }

      return receiver;
   }

   public QueueSender getSender() throws JMSException, NamingException
   {
      // Creates connect as needed
      if (!connected)
      {
         initialize();
      }

      // Creates sender as needed
      if (sender == null)
      {
         initializeSender();
      }

      return sender;
   }

   public QueueSession getSession() throws JMSException, NamingException
   {
      // Creates connect as needed
      if (!connected)
      {
         initialize();
      }

      return session;
   }

   /**
    * Sets an exception listener to this Q.
    */
   public void setExceptionListener(ExceptionListener listener)
       throws JMSException, NamingException
   {
      // Creates connect as needed
      if (!connected)
      {
         initialize();
      }

      // Sets listener
      connection.setExceptionListener(listener);
   }

   public void reset()
   {
      // Stops all incoming message
      try
      {
         if (connection != null)
         {
            connection.stop();
         }
      }
      catch (JMSException err)
      { /* ignore */
         LOGGER.error("Exception while stopping Queue Connection: "
            + err.getMessage());
      }
      // Closes all open resources
      try
      {
         if (sender != null)
         {
            sender.close();
         }
      }
      catch (JMSException err)
      { /* ignore */
         LOGGER.error("Exception while closing Queue Sender: "
            + err.getMessage());
      }
      try
      {
         if (receiver != null)
         {
            receiver.close();
         }
      }
      catch (JMSException err)
      { /* ignore */
         LOGGER.error("Exception while closing Queue Receiver: "
            + err.getMessage());
      }
      try
      {
         if (session != null)
         {
            session.close();
         }
      }
      catch (JMSException err)
      { /* ignore */
         LOGGER.error("Exception while closing Queue Session: "
            + err.getMessage());
      }
      try
      {
         if (connection != null)
         {
            connection.close();
         }
      }
      catch (JMSException err)
      { /* ignore */
         LOGGER.error("Exception while closing Queue Connection: "
            + err.getMessage());
      }
      try
      {
         if (initialContext != null)
         {
            initialContext.close();
         }
      }
      catch (NamingException err)
      { /* ignore */
         LOGGER.error("Exception while closing Initial Context: "
            + err.getMessage());
      }

      // Clears references to all resources
      initialContext = null;
      factory = null;
      connection = null;
      session = null;
      queue = null;
      receiver = null;
      sender = null;
      connected = false;
   }

   public TextMessage createTextMessage(String xmlStr, String correlationId)
       throws JMSException, NamingException
   {
      TextMessage textMessage = getSession().createTextMessage();
      textMessage.clearBody();
      textMessage.setText(xmlStr);
      textMessage.clearProperties();

      textMessage.setStringProperty("messageID", correlationId);
      textMessage.setJMSCorrelationID(correlationId);
      textMessage.setJMSExpiration(0);

      return textMessage;
   }

   public ObjectMessage createObjectMessage(Serializable obj,
       String correlationId) throws JMSException, NamingException
   {
      ObjectMessage objMessage = getSession().createObjectMessage();
      objMessage.clearBody();
      objMessage.setObject(obj);
      objMessage.clearProperties();

      objMessage.setStringProperty("messageID", correlationId);
      objMessage.setJMSCorrelationID(correlationId);

      return objMessage;
   }

   public ObjectMessage createObjectMessage(Serializable obj)
       throws JMSException, NamingException
   {
      return getSession().createObjectMessage(obj);
   }

   public void addToQueue(TextMessage textMessage) throws JMSException,
       NamingException
   {
      try
      {
         // Gets sender (and initializes any connections)
         QueueSender theSender = getSender();
         theSender.send(textMessage, DeliveryMode.NON_PERSISTENT,
            textMessage.getJMSPriority(), 0);
      }
      finally
      {
         reset();
      }
   }

   public void addToQueue(String xmlStr, String correlationId)
       throws JMSException, NamingException
   {
      try
      {
         TextMessage textMessage = getSession().createTextMessage();
         textMessage.clearBody();
         textMessage.setText(xmlStr);
         textMessage.clearProperties();

         textMessage.setStringProperty("messageID", correlationId);
         textMessage.setJMSCorrelationID(correlationId);
         textMessage.setJMSExpiration(0);

         // Gets sender (and initializes any connections)
         QueueSender theSender = getSender();
         theSender.send(textMessage, DeliveryMode.NON_PERSISTENT,
            textMessage.getJMSPriority(), 0);
      }
      finally
      {
         reset();
      }
   }

   public void sendMessage(ObjectMessage objMessage, boolean isPersistent)
       throws JMSException, NamingException
   {
      try
      {

         QueueSender theSender = getSender();
         theSender.send(objMessage,
            ((isPersistent) ? DeliveryMode.PERSISTENT
               : DeliveryMode.NON_PERSISTENT), objMessage
               .getJMSPriority(), 0);
      }
      finally
      {
         reset();
      }
   }

   public void sendMessage(ObjectMessage objMessage, int deliveryMode,
       int priority, long timeout) throws JMSException, NamingException
   {
      try
      {
         QueueSender theSender = getSender();
         theSender.send(objMessage, deliveryMode, priority, timeout);
      }
      finally
      {
         reset();
      }
   }

   /**
    * Method to override for setting any specific properties needed when creating object messages.
    */
   protected void updateMessageProperties(ObjectMessage objMessage,
       Serializable object)
   {
      // empty
   }

   public boolean isConnected()
   {
      return connected;
   }

   public String getJndiURL()
   {
      return jndiURL;
   }

   public String getPassword()
   {
      return password;
   }

   public String getQueueFactoryName()
   {
      return queueFactoryName;
   }

   public String getQueueName()
   {
      return queueName;
   }

   public String getUsername()
   {
      return username;
   }

   public boolean isTransacted()
   {
      return transacted;
   }

   public void setTransacted(boolean transacted)
   {
      this.transacted = transacted;
   }

   public void closeReceiver() throws JMSException
   {

      if (receiver != null)
         receiver.close();

      receiver = null;

   }

   public Queue getQueue()
   {
      return queue;
   }

   public void setQueue(Queue queue)
   {
      this.queue = queue;
   }

   public int getSessionAcknowledge()
   {
      return sessionAcknowledge;
   }

   public void setSessionAcknowledge(int sessionAcknowledge)
   {
      this.sessionAcknowledge = sessionAcknowledge;
   }
}
