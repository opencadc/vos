/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2018.                            (c) 2018.
*  Government of Canada                 Gouvernement du Canada
*  National Research Council            Conseil national de recherches
*  Ottawa, Canada, K1A 0R6              Ottawa, Canada, K1A 0R6
*  All rights reserved                  Tous droits réservés
*
*  NRC disclaims any warranties,        Le CNRC dénie toute garantie
*  expressed, implied, or               énoncée, implicite ou légale,
*  statutory, of any kind with          de quelque nature que ce
*  respect to the software,             soit, concernant le logiciel,
*  including without limitation         y compris sans restriction
*  any warranty of merchantability      toute garantie de valeur
*  or fitness for a particular          marchande ou de pertinence
*  purpose. NRC shall not be            pour un usage particulier.
*  liable in any event for any          Le CNRC ne pourra en aucun cas
*  damages, whether direct or           être tenu responsable de tout
*  indirect, special or general,        dommage, direct ou indirect,
*  consequential or incidental,         particulier ou général,
*  arising from the use of the          accessoire ou fortuit, résultant
*  software.  Neither the name          de l'utilisation du logiciel. Ni
*  of the National Research             le nom du Conseil National de
*  Council of Canada nor the            Recherches du Canada ni les noms
*  names of its contributors may        de ses  participants ne peuvent
*  be used to endorse or promote        être utilisés pour approuver ou
*  products derived from this           promouvoir les produits dérivés
*  software without specific prior      de ce logiciel sans autorisation
*  written permission.                  préalable et particulière
*                                       par écrit.
*
*  This file is part of the             Ce fichier fait partie du projet
*  OpenCADC project.                    OpenCADC.
*
*  OpenCADC is free software:           OpenCADC est un logiciel libre ;
*  you can redistribute it and/or       vous pouvez le redistribuer ou le
*  modify it under the terms of         modifier suivant les termes de
*  the GNU Affero General Public        la “GNU Affero General Public
*  License as published by the          License” telle que publiée
*  Free Software Foundation,            par la Free Software Foundation
*  either version 3 of the              : soit la version 3 de cette
*  License, or (at your option)         licence, soit (à votre gré)
*  any later version.                   toute version ultérieure.
*
*  OpenCADC is distributed in the       OpenCADC est distribué
*  hope that it will be useful,         dans l’espoir qu’il vous
*  but WITHOUT ANY WARRANTY;            sera utile, mais SANS AUCUNE
*  without even the implied             GARANTIE : sans même la garantie
*  warranty of MERCHANTABILITY          implicite de COMMERCIALISABILITÉ
*  or FITNESS FOR A PARTICULAR          ni d’ADÉQUATION À UN OBJECTIF
*  PURPOSE.  See the GNU Affero         PARTICULIER. Consultez la Licence
*  General Public License for           Générale Publique GNU Affero
*  more details.                        pour plus de détails.
*
*  You should have received             Vous devriez avoir reçu une
*  a copy of the GNU Affero             copie de la Licence Générale
*  General Public License along         Publique GNU Affero avec
*  with OpenCADC.  If not, see          OpenCADC ; si ce n’est
*  <http://www.gnu.org/licenses/>.      pas le cas, consultez :
*                                       <http://www.gnu.org/licenses/>.
*
*  $Revision: 4 $
*
************************************************************************
*/

package ca.nrc.cadc.vos;

import ca.nrc.cadc.vos.View.Parameter;
import ca.nrc.cadc.xml.XmlUtil;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.log4j.Logger;
import org.jdom2.Attribute;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.JDOMException;
import org.jdom2.Namespace;

/**
 * Constructs a Transfer from an XML source. This class is not thread safe but it is
 * re-usable  so it can safely be used to sequentially parse multiple XML transfer
 * documents.
 *
 * @author pdowler
 */
public class TransferReader implements XmlProcessor
{
    private static Logger log = Logger.getLogger(TransferReader.class);

    protected Map<String, String> schemaMap;

    /**
     * Constructor. XML Schema validation is enabled by default.
     */
    public TransferReader() { this(true); }

    /**
     * Constructor. XML schema validation may be disabled, in which case the client
     * is likely to fail in horrible ways (e.g. NullPointerException) if it receives
     * invalid documents. However, performance may be improved.
     *
     * @param enableSchemaValidation
     */
    public TransferReader(boolean enableSchemaValidation)
    {
        if (enableSchemaValidation)
        {
            // only use the 2.1 schema now
            //String vospaceSchemaUrl20 = XmlUtil.getResourceUrlString(VOSPACE_SCHEMA_RESOURCE_20, TransferReader.class);
            //if (vospaceSchemaUrl20 == null)
            //    throw new RuntimeException("failed to find " + VOSPACE_SCHEMA_RESOURCE_20 + " in classpath");

            String vospaceSchemaUrl21 = XmlUtil.getResourceUrlString(VOSPACE_SCHEMA_RESOURCE_21, TransferReader.class);
            if (vospaceSchemaUrl21 == null)
                throw new RuntimeException("failed to find " + VOSPACE_SCHEMA_RESOURCE_21 + " in classpath");

            this.schemaMap = new HashMap<String, String>();
            //schemaMap.put(VOSPACE_NS_20, vospaceSchemaUrl20);
            // (the namespace is 2.0 but the version (noted by an attribute) is 2.1)
            schemaMap.put(VOSPACE_NS_20, vospaceSchemaUrl21);

            log.debug("schema validation enabled");
        }
        else
            log.debug("schema validation disabled");
    }

    public Transfer read(Reader reader, String targetScheme)
        throws IOException, TransferParsingException
    {
        try
        {
            // TODO: investigate creating a SAXBuilder once and re-using it
            // as long as we can detect concurrent access (a la java collections)
            Document doc = XmlUtil.buildDocument(reader, schemaMap);
            return parseTransfer(doc, targetScheme);
        }
        catch(JDOMException ex)
        {
            throw new TransferParsingException("failed to parse XML", ex);
        }
        catch(URISyntaxException ex)
        {
            throw new TransferParsingException("invalid URI in transfer document", ex);
        }
    }

    public Transfer read(InputStream in, String targetScheme)
        throws IOException, TransferParsingException
    {
        InputStreamReader reader = new InputStreamReader(in);
        return read(reader, targetScheme);
    }

    public Transfer read(String string, String targetScheme)
        throws IOException, TransferParsingException
    {
        StringReader reader = new StringReader(string);
        return read(reader, targetScheme);
    }

    private Transfer parseTransfer(Document document, String targetScheme)
        throws URISyntaxException
    {
        Element root = document.getRootElement();
        Namespace vosNS = root.getNamespace();
        Attribute versionAttr = root.getAttribute("version");
        
        int version;
        if (VOSPACE_NS_20.equals(vosNS.getURI())) {
            version = VOS.VOSPACE_20;
            // Check the minor version attribute
            if (versionAttr != null && VOSPACE_MINOR_VERSION_21.equals(versionAttr.getValue())) {
                version = VOS.VOSPACE_21;
            }
        } else {
            throw new IllegalArgumentException("unexpected VOSpace namespace: " + vosNS.getURI());
        }

        Direction direction = parseDirection(root, vosNS);
        // String serviceUrl; // not in XML yet
        URI target = new URI(root.getChildText("target", vosNS));

        if (targetScheme != null && !targetScheme.equalsIgnoreCase(target.getScheme()))
            throw new IllegalArgumentException("Target scheme must be: " + targetScheme);

        // TODO: get view nodes and uri attribute
        View view = null;
        Parameter param = null;
        List views = root.getChildren("view", vosNS);
        if (views != null && views.size() > 0)
        {
            Element v = (Element) views.get(0);
            view = new View(new URI(v.getAttributeValue("uri")));
            List params = v.getChildren("param", vosNS);
            if (params != null)
            {
                for (Object o : params)
                {
                    Element p = (Element) o;
                    param = new Parameter(new URI(p.getAttributeValue("uri")), p.getText());
                    view.getParameters().add(param);
                }
            }
        }
        List<Protocol> protocols = parseProtocols(root, vosNS, version);
        String keepBytesStr = root.getChildText("keepBytes", vosNS);

        boolean keepBytes = true;
        if (keepBytesStr != null)
            keepBytes = keepBytesStr.equalsIgnoreCase("true");

        Transfer ret = new Transfer(target, direction, view, protocols, keepBytes);
        ret.version = version;

        // optional param(s) added in VOSpace-2.1
        if (version >= VOS.VOSPACE_21)
        {
            List<Element> params = root.getChildren("param", vosNS);
            for (Element pe : params)
            {
                String uri = pe.getAttributeValue("uri");
                if (VOS.PROPERTY_URI_CONTENTLENGTH.equals(uri))
                {
                    try
                    {
                        ret.setContentLength(new Long(pe.getText()));
                    }
                    catch(NumberFormatException ex)
                    {
                        throw new IllegalArgumentException("invalid " + VOS.PROPERTY_URI_CONTENTLENGTH
                            + ": " + pe.getText());
                    }
                }
                else
                    log.debug("skip unknown param: " + uri);
            }
        }

        return ret;
    }

    private Direction parseDirection(Element root, Namespace vosNS)
    {
        Direction rtn = null;
        String strDirection = root.getChildText("direction", vosNS);

        if (strDirection == null)
            throw new RuntimeException("Did not find direction element in XML.");

        if (strDirection.equalsIgnoreCase(Direction.pullFromVoSpace.getValue()))
            rtn = Direction.pullFromVoSpace;
        else if (strDirection.equalsIgnoreCase(Direction.pullToVoSpace.getValue()))
            rtn = Direction.pullToVoSpace;
        else if (strDirection.equalsIgnoreCase(Direction.pushFromVoSpace.getValue()))
            rtn = Direction.pushFromVoSpace;
        else if (strDirection.equalsIgnoreCase(Direction.pushToVoSpace.getValue()))
            rtn = Direction.pushToVoSpace;
        else if (strDirection.equalsIgnoreCase(Direction.BIDIRECTIONAL.getValue()))
            rtn = Direction.BIDIRECTIONAL;
        else
            rtn = new Direction(strDirection);
        return rtn;
    }

    private List<Protocol> parseProtocols(Element root, Namespace vosNS, int version)
    {
        List<Protocol> rtn = null;
        //Element e = root.getChild("protocols", NS);
        List prots = root.getChildren("protocol", vosNS);
        if (prots != null && prots.size() > 0)
        {
            rtn = new ArrayList<Protocol>(prots.size());
            for (Object obj : prots)
            {
                Element eProtocol = (Element) obj;
                String uri = eProtocol.getAttributeValue("uri");
                Protocol p = new Protocol(uri);

                // optional ndpoint
                String endpoint = eProtocol.getChildText("endpoint", vosNS);
                if (endpoint != null)
                    p.setEndpoint(endpoint);

                // optional securityMethod added in VOSpace-2.1
                if (version >= VOS.VOSPACE_21)
                {
                    Element eSec = eProtocol.getChild("securityMethod", vosNS);
                    if (eSec != null)
                    {
                        String secVal = eSec.getAttributeValue("uri");
                        try
                        {
                            p.setSecurityMethod(new URI(secVal));
                        }
                        catch(URISyntaxException ex)
                        {
                            throw new IllegalArgumentException("invalid securityMethod: " + secVal, ex);
                        }
                    }
                }
                rtn.add(p);
            }
        }
        return rtn;
    }
}
