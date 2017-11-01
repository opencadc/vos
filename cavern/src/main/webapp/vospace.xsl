<?xml version="1.0" encoding="iso-8859-1"?>
<xsl:stylesheet version="1.0"
xmlns:xsl="http://www.w3.org/1999/XSL/Transform" xmlns="http://www.ivoa.net/xml/VOSpace/v2.0" xmlns:vos="http://www.ivoa.net/xml/VOSpace/v2.0" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">

<xsl:output method="html" indent="yes" media-type="text/xml"/>

<xsl:template match="/">
 <html xmlns="http://www.w3.org/1999/xhtml">
    
     <xsl:variable name="nodeuri" select="vos:node/@uri"/>
 
     <head>
         <title>
           <xsl:value-of select="vos:node/@xsi:type"/>: <xsl:value-of select="$nodeuri"/>
         </title>
         <link href='http://fonts.googleapis.com/css?family=Nobile' rel='stylesheet' type='text/css'/>
         <style>
             body {
                 font-family: 'Nobile', serif;
                 font-size: 12px;
                 color: #5F5F5F;
             }
         </style>
     </head>
     <body>
     
		<a href="http://cadcwww.dao.nrc.ca/cadc/">
		    <img align="right" border="0" src="http://cadcwww.dao.nrc.ca/cadc/resources/images/cadc_cropped.jpg"/>
		</a>
		
		<h1>
		    <xsl:value-of select="vos:node/@xsi:type"/>: <xsl:value-of select="$nodeuri"/>
		</h1>
		
		<xsl:if test="vos:node/@xsi:type='vos:DataNode'">
		
		    <xsl:variable name="relativeNodeUrl">
		        <xsl:call-template name="lastElementOnPath">
		            <xsl:with-param name="inputString" select="$nodeuri"/>
		        </xsl:call-template>
		    </xsl:variable>
		
		    <xsl:variable name="downloadurl">
                <xsl:value-of select="$relativeNodeUrl"/>
                <xsl:value-of select="'?view=data'"/>
            </xsl:variable>
            
		    <a href="{$downloadurl}">
		        <img border="0" alt="download file" title="download file" src="/vospace/download-large.jpg"/>
		    </a>
		</xsl:if>
     
		<xsl:apply-templates select="vos:node/vos:properties"/>
		<xsl:apply-templates select="vos:node/vos:nodes"/>
		<xsl:apply-templates select="vos:node/vos:accepts"/>
		<xsl:apply-templates select="vos:node/vos:provides"/>
		
		<hr/>
         
     </body>
 </html>
</xsl:template>

<xsl:template match="vos:node/vos:properties">
    <div xmlns="http://www.w3.org/1999/xhtml">
     <h2>Properties</h2>
     <table border="0" cellspacing="4">
         <tr>
            <th align="left">
                Property URI
            </th>
            <th align="left">
                Value
            </th>
         </tr>
         <xsl:apply-templates select="vos:property"/>
     </table>
   </div>
</xsl:template>

 <xsl:template match="//vos:property">
     <tr xmlns="http://www.w3.org/1999/xhtml">
        <td>
         <xsl:value-of select="@uri"/>
        </td>
        <td>
         <xsl:value-of select="."/>
        </td>
   </tr>
 </xsl:template>

<xsl:template match="vos:node/vos:nodes">
    <div xmlns="http://www.w3.org/1999/xhtml">
     <h2>Child Nodes</h2>
     <ul>
         <xsl:apply-templates select="vos:node"/>
   </ul>         
   </div>
</xsl:template>

<xsl:template match="//vos:node">
    
    <xsl:variable name="nodeuri" select="//vos:node/@uri"/>

    <xsl:variable name="relativeChildNodeUrl">
        <xsl:call-template name="lastElementOnPath">
	        <xsl:with-param name="inputString" select="$nodeuri"/>
        </xsl:call-template>
        <xsl:value-of select="'/'"/>
        <xsl:call-template name="lastElementOnPath">
            <xsl:with-param name="inputString" select="@uri"/>
        </xsl:call-template>
    </xsl:variable>
 
    <li xmlns="http://www.w3.org/1999/xhtml">
        <a href="{$relativeChildNodeUrl}">
            <xsl:value-of select="@uri"/>
        </a>
    </li>
</xsl:template>

<xsl:template name="lastElementOnPath">
    <xsl:param name="inputString"/>
    <xsl:choose>
	    <xsl:when test="contains($inputString, '/')">
	       <xsl:call-template name="lastElementOnPath">
	           <xsl:with-param name="inputString" select="substring-after($inputString, '/')"/>
	       </xsl:call-template>
	    </xsl:when>
	    <xsl:otherwise>
	       <xsl:value-of select="$inputString"/>
	    </xsl:otherwise>
	</xsl:choose>
</xsl:template>
 
<xsl:template match="vos:node/vos:accepts">
    <div xmlns="http://www.w3.org/1999/xhtml">
     <h2>Accepts Views</h2>
     <ul>
         <xsl:apply-templates select="vos:view">
             <xsl:with-param name="link" select="'false'"/>
         </xsl:apply-templates>
   </ul>
   </div>
</xsl:template>

<xsl:template match="vos:node/vos:provides">
    <div xmlns="http://www.w3.org/1999/xhtml">
     <h2>Provides Views</h2>
     <ul>
         <xsl:apply-templates select="vos:view">
             <xsl:with-param name="link" select="'true'"/>
         </xsl:apply-templates>
   </ul>
   </div>
</xsl:template>

<xsl:template match="//vos:view">
    <xsl:param name="link"/>
    
    <xsl:variable name="nodeuri" select="vos:node/@uri"/>
    
    <xsl:if test="$link='true'">
         <li xmlns="http://www.w3.org/1999/xhtml">
             <xsl:variable name="viewurl">
                <xsl:value-of select="substring-after($nodeuri, '!vospace')"/>
                <xsl:value-of select="'?view='"/>
                <xsl:value-of select="@uri"/>
             </xsl:variable>
             <xsl:choose>
	             <xsl:when test="@uri='ivo://cadc.nrc.ca/vospace/core#dataview'">
		             <xsl:variable name="dataviewurl">
		                <xsl:value-of select="substring-after($nodeuri, '!vospace')"/>
		                <xsl:value-of select="'?view=data'"/>
		             </xsl:variable>
	                data 
	                <a href="{$dataviewurl}">
	                    <img border="0" align="bottom" src="/vospace/download-small.jpg"/>
	                </a>
	             </xsl:when>
                 <xsl:when test="@uri='ivo://cadc.nrc.ca/vospace/core#rssview'">
                     <xsl:variable name="rssviewurl">
                        <xsl:value-of select="substring-after($nodeuri, '!vospace')"/>
                        <xsl:value-of select="'?view=rss'"/>
                     </xsl:variable>
                    rss 
                    <a href="{$rssviewurl}">
                        <img border="0" align="bottom" src="/vospace/rss-small.jpg"/>
                    </a>
                 </xsl:when>
                 <xsl:when test="@uri='ivo://ivoa.net/vospace/core#defaultview'">
                     <xsl:variable name="defaultviewurl">
                        <xsl:value-of select="substring-after($nodeuri, '!vospace')"/>
                     </xsl:variable>
                    default (<a href="{$defaultviewurl}">this page</a>)
                 </xsl:when>
                 <xsl:otherwise>
		             <a href="{$viewurl}">
		                 <xsl:value-of select="@uri"/>
		             </a>
		         </xsl:otherwise>
		     </xsl:choose>
          </li>
      </xsl:if>
      <xsl:if test="$link='false'">
          <xsl:value-of select="@uri"/>
      </xsl:if>
</xsl:template>

</xsl:stylesheet>