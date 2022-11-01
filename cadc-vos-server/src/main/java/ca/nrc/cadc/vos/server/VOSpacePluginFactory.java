package ca.nrc.cadc.vos.server;

import java.net.URL;
import java.util.List;

import org.apache.log4j.Logger;

import ca.nrc.cadc.util.MultiValuedProperties;
import ca.nrc.cadc.vos.server.db.NodePersistence;
import ca.nrc.cadc.vos.server.transfers.TransferGenerator;

public class VOSpacePluginFactory
{

    private static final String CONFIG_FILE = "VOSpacePlugins.properties";

    private static Logger log = Logger.getLogger(VOSpacePluginFactory.class);

    public VOSpacePluginFactory()
    {
    }

    public TransferGenerator createTransferGenerator()
    {
        String className = null;
        try
        {
            className = getImplementingClassName(TransferGenerator.class);
            Class<TransferGenerator> tgClass = (Class<TransferGenerator>) Class.forName(className);
            return tgClass.newInstance();
        }
        catch (Exception e)
        {
            log.error("Failed instantiate class: " + className, e);
            throw new RuntimeException(e);
        }
    }

    public NodePersistence createNodePersistence()
    {
        String className = null;
        try
        {
            className = getImplementingClassName(NodePersistence.class);
            Class<NodePersistence> tgClass = (Class<NodePersistence>) Class.forName(className);
            return tgClass.newInstance();
        }
        catch (Exception e)
        {
            log.error("Failed instantiate class: " + className, e);
            throw new RuntimeException(e);
        }
    }

    private String getImplementingClassName(Class clazz)
    {
        try
        {
            URL urlToProperties = VOSpacePluginFactory.class.getClassLoader().getResource(CONFIG_FILE);
            log.debug("URL for VOSpacePlugins.properties: " + urlToProperties.toString());
            MultiValuedProperties mvp = new MultiValuedProperties();
            mvp.load(urlToProperties.openStream());
            List<String> classes = mvp.getProperty(clazz.getName());

            if (classes == null || classes.isEmpty())
            {
                throw new IllegalStateException("No class configured");
            }
            if (classes.size() > 1)
            {
                throw new IllegalStateException("Only one implementing class allowed");
            }

            return classes.get(0);
        }
        catch (Exception e)
        {
            log.error("Failed to plugin class " + clazz.getName(), e);
            throw new RuntimeException(e);
        }
    }

}
