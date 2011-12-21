<%@ page
  contentType="text/html; charset=UTF-8"
  import="javax.servlet.*"
  import="javax.servlet.http.*"
  import="java.io.*"
  import="java.util.*"
  import="org.apache.hadoop.raid.*"
  import="org.apache.hadoop.raid.StatisticsCollector"
  import="org.apache.hadoop.util.*"
  import="org.apache.hadoop.hdfs.*"
  import="org.apache.hadoop.hdfs.DistributedFileSystem.*"
  import="java.lang.Integer"
  import="java.text.SimpleDateFormat"
%>
<%
  RaidNode raidNode = (RaidNode) application.getAttribute("raidnode");
  StatisticsCollector stats = (StatisticsCollector) raidNode
      .getStatsCollector();
  PurgeMonitor purge = raidNode.getPurgeMonitor();
  PlacementMonitor place = raidNode.getPlacementMonitor();
  DiskStatus ds = new DFSClient(raidNode.getConf()).getDiskStatus();
  String name = raidNode.getHostName();
  name = name.substring(0, name.indexOf(".")).toUpperCase();
%>
<%!
  private String td(String s) {
    return JspUtils.td(s);
  }

  private String tr(String s) {
    return JspUtils.tr(s);
  }

  private String table(String s) {
    return JspUtils.tableSimple(s);
  }
  private long now() {
    return System.currentTimeMillis();
  }
%>

<html>
  <head>
    <title><%=name%> Hadoop RaidNode Administration</title> <link rel="stylesheet" type="text/css" href="/static/hadoop.css">
  </head>
<body>
<h1><%=name%> Hadoop RaidNode Administration</h1>
<b>Started:</b> <%=new Date(raidNode.getStartTime())%><br>
<b>Version:</b> <%=VersionInfo.getVersion()%>,
                r<%=VersionInfo.getRevision()%><br>
<b>Compiled:</b> <%=VersionInfo.getDate()%> by
                 <%=VersionInfo.getUser()%><br>
<hr>
<h2>RAID Summary </h2>
<%
  String total = StringUtils.byteDesc(ds.getCapacity());
  String used = StringUtils.byteDesc(ds.getDfsUsed());
  String saving = StringUtils.byteDesc(stats.getSaving());
  String doneSaving = StringUtils.byteDesc(stats.getDoneSaving());
  String repl = StringUtils
      .limitDecimalTo2(stats.getEffectiveReplication());
  String lastUpdate =
      StringUtils.formatTime(now() - stats.getLastUpdateTime()) + " ago";
  String updateUsed = StringUtils.formatTime(stats.getUpdateUsedTime());
  Thread.State state = raidNode.getStatsCollectorState();
  String filesScanned = StringUtils.humanReadableInt(stats
      .getFilesScanned());
  String tableStr = "";
  if (stats.getLastUpdateTime() != 0L) {
    tableStr += tr(td("Effective Replication") + td(":") + td(repl));
    tableStr += tr(td("Total") + td(":") + td(total));
    tableStr += tr(td("Used") + td(":") + td(used));
    tableStr += tr(td("Saving") + td(":") + td(saving));
    tableStr += tr(td("Done Saving") + td(":") + td(doneSaving));
    tableStr += tr(td("File Scanned") + td(":") + td(filesScanned));
    tableStr += tr(td("Update Used") + td(":") + td(updateUsed));
    tableStr += tr(td("Last Update") + td(":") + td(lastUpdate));
  } else {
    tableStr += tr(td("Total") + td(":") + td(total));
    tableStr += tr(td("Used") + td(":") + td(used));
    tableStr += tr(td("File Scanned") + td(":") + td(filesScanned));
  }
  out.print(table(tableStr));
%>

<% for (Codec c : Codec.getCodecs()) {
  out.print("<hr>");
  out.print("<h2>" + c.id.toUpperCase() + "</h2>");
  String paritySize, estParitySize;
  Statistics st = stats.getRaidStatistics(c.id);
  if (st != null) {
    out.print(st.htmlTable());
    saving = StringUtils.byteDesc(st.getSaving());
    doneSaving = StringUtils.byteDesc(st.getDoneSaving());
    repl = StringUtils.limitDecimalTo2(st.getEffectiveReplication());
    paritySize = StringUtils.byteDesc(st.getParityCounters()
        .getNumBytes());
    estParitySize = StringUtils.byteDesc(st.getEstimatedParitySize());
    tableStr = "";
    tableStr += tr(td("Effective Replication") + td(":") + td(repl));
    tableStr += tr(td("Saving") + td(":") + td(saving));
    tableStr += tr(td("Done Saving") + td(":") + td(doneSaving));
    tableStr += tr(td("Parity / Expected") + td(":")
        + td(paritySize + " / " + estParitySize));
    out.print(table(tableStr));
  } else {
    out.print("Wait for collecting");
  }
  }
%>

<hr>
<h2>Block Placement</h2>
<%
  if (place.lastUpdateTime() != 0) {
    out.print(place.htmlTable());
    tableStr = "";
    lastUpdate =
        StringUtils.formatTime(now() - place.lastUpdateTime()) + " ago";
    updateUsed = StringUtils.formatTime(place.lastUpdateUsedTime());
    String queueSize = StringUtils.humanReadableInt(place
        .getMovingQueueSize());
    tableStr += tr(td("Moving in Progress") + td(":") + td(queueSize));
    tableStr += tr(td("Update Used") + td(":") + td(updateUsed));
    tableStr += tr(td("Last Update") + td(":") + td(lastUpdate));
    out.print(table(tableStr));
  } else {
    String queueSize = StringUtils.humanReadableInt(place
        .getMovingQueueSize());
    tableStr = tr(td("Moving in Progress") + td(":") + td(queueSize));
    out.print(table(tableStr));
  }
%>
<%
  BlockIntegrityMonitor.Status status = null;
  boolean unsupported = false;
  try {
    status = raidNode.getBlockIntegrityMonitorStatus();
  } catch (UnsupportedOperationException e) {
    unsupported = true;
  }
  if (!unsupported) {
    out.print("<hr>\n");
    out.print("<h2>Block Fixing "
        + JspUtils.link("see details", "blockfixer.jsp") + "</h2>");
    if (status != null) {
      out.print(status.toHtml(0));
    } else {
      out.print("Wait for collecting");
    }
  }
%>
<%
  out.print("<hr>\n");
  out.print("<h2>Raid Jobs "
      + JspUtils.link("see details", "jobmonitor.jsp") + "</h2>");
%>
<%
  out.println(ServletUtil.htmlFooter());
%>
