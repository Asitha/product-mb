package org.wso2.mb.integration.tests.amqp.functional;

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.wso2.carbon.automation.engine.context.TestUserMode;
import org.wso2.carbon.automation.engine.context.beans.User;
import org.wso2.carbon.integration.common.admin.client.UserManagementClient;
import org.wso2.carbon.user.mgt.stub.UserAdminUserAdminException;
import org.wso2.mb.integration.common.utils.JMSClientHelper;
import org.wso2.mb.integration.common.utils.backend.MBIntegrationBaseTest;

import java.rmi.RemoteException;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.Queue;
import javax.jms.QueueConnection;
import javax.jms.QueueConnectionFactory;
import javax.jms.QueueSender;
import javax.jms.QueueSession;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.xml.xpath.XPathExpressionException;

/**
 * Test publish permissions when there is only either queue create publish or consume permission is given for a
 * user for a particular queue or topic.
 */
public class EI2717PublishPermissionTestCase extends MBIntegrationBaseTest {

    private static final String USER = "EI2717user";
    private static final String PASSWORD = "EI2717user";
    private static final String QUEUE_NAME = "EI2717Queue";
    private static final String ROLE_NAME = "EI2717Role";
    /**
     * Permission path for creating a queue.
     */
    private static final String ADD_QUEUE_PERMISSION = "/permission/admin/manage/queue/add";

    private QueueConnection queueConnection;
    private QueueSession queueSession;
    private UserManagementClient userManagementClient;

    @BeforeClass
    public void setUp()
            throws XPathExpressionException, NamingException, JMSException, RemoteException,
                   UserAdminUserAdminException {
        super.init(TestUserMode.SUPER_TENANT_ADMIN);
        User adminUser = getSuperTenantAdminUser();

        setupRoles(adminUser);
        String adminUserName = adminUser.getUserName();
        String adminPassword = adminUser.getPassword();
        JMSClientHelper.InitialContextBuilder contextBuilder =
                JMSClientHelper.createInitialContextBuilder(adminUserName, adminPassword, getBrokerHost(), getAMQPPort());

        InitialContext initialContext = contextBuilder.build();
        ConnectionFactory connectionFactory =
                (ConnectionFactory)initialContext.lookup(JMSClientHelper.QUEUE_CONNECTION_FACTORY);
        QueueConnectionFactory queueConnectionFactory = (QueueConnectionFactory) connectionFactory;

        queueConnection = queueConnectionFactory.createQueueConnection();
        queueSession = queueConnection.createQueueSession(false, QueueSession.AUTO_ACKNOWLEDGE);
        queueSession.createQueue(QUEUE_NAME);

    }

    private void setupRoles(User adminUser) throws UserAdminUserAdminException, RemoteException {
        String roleName = "EI2717Role";
        String[] userList = new String[] {USER};
        // Logging into user management as admin
        userManagementClient = new UserManagementClient(backendURL, adminUser.getUserName(),
                                                                             adminUser.getPassword());

        userManagementClient.addUser(USER, PASSWORD, null, null);
        // Adding roles along with users if roles does not exist.
        userManagementClient.addRole(roleName, userList, new String[]{ADD_QUEUE_PERMISSION});
    }

    @AfterClass
    public void tearDown() throws JMSException, RemoteException, UserAdminUserAdminException {
        queueSession.close();
        queueConnection.close();

        userManagementClient.deleteUser(USER);
        userManagementClient.deleteRole(ROLE_NAME);
    }

    @Test(groups = { "wso2.mb", "queue", "patches" })
    public void testPublishPermissionForQueue() throws NamingException, XPathExpressionException, JMSException {
        JMSClientHelper.InitialContextBuilder contextBuilder =
                JMSClientHelper.createInitialContextBuilder(USER, PASSWORD, getBrokerHost(), getAMQPPort());

        InitialContext initialContext = contextBuilder.withQueue(QUEUE_NAME).build();

        ConnectionFactory connectionFactory =
                (ConnectionFactory)initialContext.lookup(JMSClientHelper.QUEUE_CONNECTION_FACTORY);
        QueueConnectionFactory queueConnectionFactory = (QueueConnectionFactory) connectionFactory;
        QueueConnection senderConnection = queueConnectionFactory.createQueueConnection();
        QueueSession senderSession = senderConnection.createQueueSession(false, QueueSession.AUTO_ACKNOWLEDGE);

        Queue queue = senderSession.createQueue(QUEUE_NAME);
        QueueSender sender = senderSession.createSender(queue);

        Assert.assertNotNull(sender);

        sender.close();
        senderSession.close();
        senderConnection.close();

    }
}
