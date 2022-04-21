/*
 * Copyright 2021 DataCanvas
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package listener;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.Comparator;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.Set;

import io.dingodb.dailytest.CommonArgs;
import utils.SendEmailClient;
import jakarta.mail.MessagingException;
import org.testng.collections.Lists;
import org.testng.internal.Utils;
import org.testng.log4testng.Logger;
import org.testng.IReporter;
import org.testng.ISuite;
import org.testng.ISuiteResult;
import org.testng.ITestContext;
import org.testng.ITestResult;
import org.testng.Reporter;
import org.testng.xml.XmlSuite;
import org.testng.xml.XmlSuite.ParallelMode;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.Files.newBufferedWriter;


public class EmailableReporterListener implements IReporter{
    private static final Logger LOG = Logger.getLogger(EmailableReporterListener.class);

    protected PrintWriter writer;

    protected final List<EmailableReporterListener.SuiteResult> suiteResults = Lists.newArrayList();

    // Reusable buffer
    private final StringBuilder buffer = new StringBuilder();

    private String fileName = "DingoDB-test-report.html";
//    private String hostIP = "172.20.3.26";
//    private String hostIP = System.getenv("ConnectIP");
    private String hostIP = CommonArgs.getDefaultDingoClusterIP();

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public String getFileName() {
        return fileName;
    }

    @Override
    public void generateReport(
        List<XmlSuite> xmlSuites, List<ISuite> suites, String outputDirectory) {
        Date date = new Date();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        String dateNowStr = sdf.format(date);

//        try {
//            writer = createWriter(outputDirectory);
//        } catch (IOException e) {
//            LOG.error("Unable to create output file", e);
//            return;
//        }

        if (! new File("./ReportOut/" + dateNowStr).exists()) {
            if (! new File("./ReportOut/" + dateNowStr).mkdirs()) {
                throw new RuntimeException("创建测试报告目录失败！");
            }
            try {
                writer = new PrintWriter(newBufferedWriter(new File("./ReportOut/" + dateNowStr + "/", fileName).toPath(), UTF_8));
            } catch (IOException e) {
                throw new RuntimeException("创建测试报告文件失败！");
            }
        }
        else {
            try {
                File file = new File("./ReportOut/" + dateNowStr + "/", fileName);
                if (file.exists()) {
                    file.delete();
                    writer = new PrintWriter(newBufferedWriter(new File("./ReportOut/" + dateNowStr + "/", fileName).toPath(), UTF_8));
                }
                else {
                    writer = new PrintWriter(newBufferedWriter(new File("./ReportOut/" + dateNowStr + "/", fileName).toPath(), UTF_8));
                }
            } catch (IOException e) {
                throw new RuntimeException("创建测试报告文件失败！");
            }
        }

        for (ISuite suite : suites) {
            suiteResults.add(new SuiteResult(suite));
        }

        writeDocumentStart();
        writeHead();
        writeBody();
        writeDocumentEnd();

        writer.close();


        SendEmailClient sendEmailClient = new SendEmailClient.SendEmailClientBuilder().build();
        try {
            Thread.sleep(3000);
            sendEmailClient.sendHTMLEmail("./ReportOut/" + dateNowStr + "/" + fileName);
        } catch (InterruptedException | MessagingException | IOException | NullPointerException e) {
            e.printStackTrace();
        }
        /*
        String str = "-yyyyMMddHHmmss";
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(str);
        try {
            Thread.sleep(3000);
            sendEmailClient.sendHTMLEmail("liwt@zetyun.com", "liwt@zetyun.com", "DingoDB每日BVT测试报告"
                + simpleDateFormat.format(date), ".\\ReportOut\\" + dateNowStr + "\\" + fileName);
        } catch (InterruptedException | MessagingException e) {
            e.printStackTrace();
        }
         */
    }
    /*
    protected PrintWriter createWriter(String outdir) throws IOException {
        new File(outdir).mkdirs();
        String jvmArg = RuntimeBehavior.getDefaultEmailableReport2Name();
        if (jvmArg != null && !jvmArg.trim().isEmpty()) {
            fileName = jvmArg;
        }
        return new PrintWriter(newBufferedWriter(new File(outdir, fileName).toPath(), UTF_8));
    }
    */

    protected void writeDocumentStart() {
        writer.println(
            "<!DOCTYPE html PUBLIC \"-//W3C//DTD XHTML 1.1//EN\" \"http://www.w3.org/TR/xhtml11/DTD/xhtml11.dtd\">");
        writer.println("<html xmlns=\"http://www.w3.org/1999/xhtml\">");
    }

    protected void writeHead() {
        writer.println("<head>");
        writer.println("<meta http-equiv=\"content-type\" content=\"text/html; charset=UTF-8\"/>");
        writer.println("<title>DingoDB每日BVT测试报告</title>");
        writeStylesheet();
        writer.println("</head>");
    }

    protected void writeStylesheet() {
        writer.print("<style type=\"text/css\">");
        writer.print("table {margin-bottom:10px;border-collapse:collapse;empty-cells:show}");
        writer.print("th,td {border:1px solid #009;padding:.25em .5em}");
        writer.print("th {vertical-align:bottom}");
        writer.print("td {vertical-align:top}");
        writer.print("table a {font-weight:bold}");
        writer.print(".stripe td {background-color: #E6EBF9}");
        writer.print(".num {text-align:right}");
        writer.print(".passedodd td {background-color: #3F3}");
        writer.print(".passedeven td {background-color: #0A0}");
        writer.print(".skippedodd td,.atts {background-color: #FB1}");
        writer.print(".skippedeven td,.strip .atts {background-color: #FB1}");
        writer.print(".failedodd td,.attn {background-color: #E22}");
        writer.print(".failedeven td,.stripe .attn {background-color: #E22}");
        writer.print(".stacktrace {white-space:pre;font-family:monospace}");
        writer.print(".totop {font-size:85%;text-align:center;border-bottom:2px solid #000}");
        writer.print(".invisible {display:none}");
        writer.println("</style>");
    }

    protected void writeBody() {
        Date date = new Date();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String dateNowStr = sdf.format(date);
        writer.println("<body>");
        writer.println("<h1>DingoDB测试报告</h1>");
        writer.println();
        writer.println("<h4>. 测试日期：" + dateNowStr + "</h4>");
        writer.println("<h4>. 测试节点IP：" + hostIP + "</h4>");
        writeSuiteSummary();
        writeScenarioSummary();
        writeScenarioDetails();
        writer.println("</body>");
    }

    protected void writeDocumentEnd() {
        writer.println("</html>");
    }

    protected void writeSuiteSummary() {
        NumberFormat integerFormat = NumberFormat.getIntegerInstance();
        NumberFormat decimalFormat = NumberFormat.getNumberInstance();

        int totalPassedTests = 0;
        int totalSkippedTests = 0;
        int totalFailedTests = 0;
        int totalRetriedTests = 0;
        long totalDuration = 0;

        writer.println("<table>");
        writer.print("<tr>");
        writer.print("<th>Test</th>");
        writer.print("<th># Passed</th>");
        writer.print("<th># Skipped</th>");
        writer.print("<th># Retried</th>");
        writer.print("<th># Failed</th>");
        writer.print("<th>Time (ms)</th>");
        writer.print("<th>Included Groups</th>");
        writer.print("<th>Excluded Groups</th>");
        writer.println("</tr>");

        int testIndex = 0;
        for (SuiteResult suiteResult : suiteResults) {
            writer.print("<tr><th colspan=\"8\">");
            writer.print(Utils.escapeHtml(suiteResult.getSuiteName()));
            writer.println("</th></tr>");

            for (TestResult testResult : suiteResult.getTestResults()) {
                int passedTests = testResult.getPassedTestCount();
                int skippedTests = testResult.getSkippedTestCount();
                int failedTests = testResult.getFailedTestCount();
                int retriedTests = testResult.getRetriedTestCount();
                long duration = testResult.getDuration();

                writer.print("<tr");
                if ((testIndex % 2) == 1) {
                    writer.print(" class=\"stripe\"");
                }
                writer.print(">");

                buffer.setLength(0);
                writeTableData(
                    buffer
                        .append("<a href=\"#t")
                        .append(testIndex)
                        .append("\">")
                        .append(Utils.escapeHtml(testResult.getTestName()))
                        .append("</a>")
                        .toString());
                writeTableData(integerFormat.format(passedTests), "num");
                writeTableData(integerFormat.format(skippedTests), (skippedTests > 0 ? "num atts" : "num"));
                writeTableData(integerFormat.format(retriedTests), (retriedTests > 0 ? "num attn" : "num"));
                writeTableData(integerFormat.format(failedTests), (failedTests > 0 ? "num attn" : "num"));
                writeTableData(decimalFormat.format(duration), "num");
                writeTableData(testResult.getIncludedGroups());
                writeTableData(testResult.getExcludedGroups());

                writer.println("</tr>");

                totalPassedTests += passedTests;
                totalSkippedTests += skippedTests;
                totalFailedTests += failedTests;
                totalRetriedTests += retriedTests;
                totalDuration += duration;

                testIndex++;
            }
            boolean testsInParallel = ParallelMode.TESTS.equals(suiteResult.getParallelMode());
            if (testsInParallel) {
                Optional<TestResult> maxValue = suiteResult.testResults.stream()
                    .max(Comparator.comparing(TestResult::getDuration));
                if (maxValue.isPresent()) {
                    totalDuration = Math.max(totalDuration, maxValue.get().duration);
                }
            }
        }

        // Print totals if there was more than one test
        if (testIndex > 1) {
            writer.print("<tr>");
            writer.print("<th>Total</th>");
            writeTableHeader(integerFormat.format(totalPassedTests), "num");
            writeTableHeader(
                integerFormat.format(totalSkippedTests), (totalSkippedTests > 0 ? "num attn" : "num"));
            writeTableHeader(
                integerFormat.format(totalRetriedTests), (totalRetriedTests > 0 ? "num attn" : "num"));
            writeTableHeader(
                integerFormat.format(totalFailedTests), (totalFailedTests > 0 ? "num attn" : "num"));
            writeTableHeader(decimalFormat.format(totalDuration), "num");
            writer.print("<th colspan=\"2\"></th>");
            writer.println("</tr>");
        }

        writer.println("</table>");
    }

    protected void writeScenarioSummary() {
        writer.print("<table id='summary'>");
        writer.print("<thead>");
        writer.print("<tr>");
        writer.print("<th>Class</th>");
        writer.print("<th>Method</th>");
        writer.print("<th>Start</th>");
        writer.print("<th>Time (ms)</th>");
        writer.print("</tr>");
        writer.print("</thead>");

        int testIndex = 0;
        int scenarioIndex = 0;
        for (SuiteResult suiteResult : suiteResults) {
            writer.print("<tbody><tr><th colspan=\"4\">");
            writer.print(Utils.escapeHtml(suiteResult.getSuiteName()));
            writer.print("</th></tr></tbody>");

            for (TestResult testResult : suiteResult.getTestResults()) {
                writer.printf("<tbody id=\"t%d\">", testIndex);

                String testName = Utils.escapeHtml(testResult.getTestName());
                int startIndex = scenarioIndex;

                scenarioIndex +=
                    writeScenarioSummary(
                        testName + " &#8212; failed (configuration methods)",
                        testResult.getFailedConfigurationResults(),
                        "failed",
                        scenarioIndex);
                scenarioIndex +=
                    writeScenarioSummary(
                        testName + " &#8212; failed",
                        testResult.getFailedTestResults(),
                        "failed",
                        scenarioIndex);
                scenarioIndex +=
                    writeScenarioSummary(
                        testName + " &#8212; skipped (configuration methods)",
                        testResult.getSkippedConfigurationResults(),
                        "skipped",
                        scenarioIndex);
                scenarioIndex +=
                    writeScenarioSummary(
                        testName + " &#8212; skipped",
                        testResult.getSkippedTestResults(),
                        "skipped",
                        scenarioIndex);
                scenarioIndex +=
                    writeScenarioSummary(
                        testName + " &#8212; retried",
                        testResult.getRetriedTestResults(),
                        "retried",
                        scenarioIndex);
                scenarioIndex +=
                    writeScenarioSummary(
                        testName + " &#8212; passed",
                        testResult.getPassedTestResults(),
                        "passed",
                        scenarioIndex);

                if (scenarioIndex == startIndex) {
                    writer.print("<tr><th colspan=\"4\" class=\"invisible\"/></tr>");
                }

                writer.println("</tbody>");

                testIndex++;
            }
        }

        writer.println("</table>");
    }

    private int writeScenarioSummary(
        String description,
        List<ClassResult> classResults,
        String cssClassPrefix,
        int startingScenarioIndex) {
        int scenarioCount = 0;
        if (!classResults.isEmpty()) {
            writer.print("<tr><th colspan=\"4\">");
            writer.print(description);
            writer.print("</th></tr>");

            int scenarioIndex = startingScenarioIndex;
            int classIndex = 0;
            for (ClassResult classResult : classResults) {
                String cssClass = cssClassPrefix + ((classIndex % 2) == 0 ? "even" : "odd");

                buffer.setLength(0);

                int scenariosPerClass = 0;
                int methodIndex = 0;
                for (MethodResult methodResult : classResult.getMethodResults()) {
                    List<ITestResult> results = methodResult.getResults();
                    int resultsCount = results.size();
                    assert resultsCount > 0;

                    ITestResult firstResult = results.iterator().next();
                    String methodName = Utils.escapeHtml(firstResult.getMethod().getMethodName());
                    long start = firstResult.getStartMillis();
                    long duration = firstResult.getEndMillis() - start;

                    // The first method per class shares a row with the class
                    // header
                    if (methodIndex > 0) {
                        buffer.append("<tr class=\"").append(cssClass).append("\">");
                    }

                    // Write the timing information with the first scenario per
                    // method
                    buffer
                        .append("<td><a href=\"#m")
                        .append(scenarioIndex)
                        .append("\">")
                        .append(methodName)
                        .append("</a></td>")
                        .append("<td rowspan=\"")
                        .append(resultsCount)
                        .append("\">")
                        .append(start)
                        .append("</td>")
                        .append("<td rowspan=\"")
                        .append(resultsCount)
                        .append("\">")
                        .append(duration)
                        .append("</td></tr>");
                    scenarioIndex++;

                    // Write the remaining scenarios for the method
                    for (int i = 1; i < resultsCount; i++) {
                        buffer
                            .append("<tr class=\"")
                            .append(cssClass)
                            .append("\">")
                            .append("<td><a href=\"#m")
                            .append(scenarioIndex)
                            .append("\">")
                            .append(methodName)
                            .append("</a></td></tr>");
                        scenarioIndex++;
                    }

                    scenariosPerClass += resultsCount;
                    methodIndex++;
                }

                // Write the test results for the class
                writer.print("<tr class=\"");
                writer.print(cssClass);
                writer.print("\">");
                writer.print("<td rowspan=\"");
                writer.print(scenariosPerClass);
                writer.print("\">");
                writer.print(Utils.escapeHtml(classResult.getClassName()));
                writer.print("</td>");
                writer.print(buffer);

                classIndex++;
            }
            scenarioCount = scenarioIndex - startingScenarioIndex;
        }
        return scenarioCount;
    }

    protected void writeScenarioDetails() {
        int scenarioIndex = 0;
        for (SuiteResult suiteResult : suiteResults) {
            for (TestResult testResult : suiteResult.getTestResults()) {
                writer.print("<h2>");
                writer.print(Utils.escapeHtml(testResult.getTestName()));
                writer.print("</h2>");

                scenarioIndex +=
                    writeScenarioDetails(testResult.getFailedConfigurationResults(), scenarioIndex);
                scenarioIndex += writeScenarioDetails(testResult.getFailedTestResults(), scenarioIndex);
                scenarioIndex +=
                    writeScenarioDetails(testResult.getSkippedConfigurationResults(), scenarioIndex);
                scenarioIndex += writeScenarioDetails(testResult.getSkippedTestResults(), scenarioIndex);
                scenarioIndex += writeScenarioDetails(testResult.getRetriedTestResults(), scenarioIndex);
                scenarioIndex += writeScenarioDetails(testResult.getPassedTestResults(), scenarioIndex);
            }
        }
    }

    private int writeScenarioDetails(List<ClassResult> classResults, int startingScenarioIndex) {
        int scenarioIndex = startingScenarioIndex;
        for (ClassResult classResult : classResults) {
            String className = classResult.getClassName();
            for (MethodResult methodResult : classResult.getMethodResults()) {
                List<ITestResult> results = methodResult.getResults();
                assert !results.isEmpty();

                String label =
                    Utils.escapeHtml(
                        className + "#" + results.iterator().next().getMethod().getMethodName());
                for (ITestResult result : results) {
                    writeScenario(scenarioIndex, label, result);
                    scenarioIndex++;
                }
            }
        }

        return scenarioIndex - startingScenarioIndex;
    }

    private void writeScenario(int scenarioIndex, String label, ITestResult result) {
        writer.print("<h3 id=\"m");
        writer.print(scenarioIndex);
        writer.print("\">");
        writer.print(label);
        writer.print("</h3>");

        writer.print("<table class=\"result\">");

        boolean hasRows = false;

        // Write test parameters (if any)
        Object[] parameters = result.getParameters();
        int parameterCount = (parameters == null ? 0 : parameters.length);
        hasRows = dumpParametersInfo("Factory Parameter", result.getFactoryParameters());
        parameters = result.getParameters();
        parameterCount = (parameters == null ? 0 : parameters.length);
        hasRows = dumpParametersInfo("Parameter", result.getParameters());

        // Write reporter messages (if any)
        List<String> reporterMessages = Reporter.getOutput(result);
        if (!reporterMessages.isEmpty()) {
            writer.print("<tr><th");
            if (parameterCount > 1) {
                writer.printf(" colspan=\"%d\"", parameterCount);
            }
            writer.print(">Messages</th></tr>");

            writer.print("<tr><td");
            if (parameterCount > 1) {
                writer.printf(" colspan=\"%d\"", parameterCount);
            }
            writer.print(">");
            writeReporterMessages(reporterMessages);
            writer.print("</td></tr>");
            hasRows = true;
        }

        // Write exception (if any)
        Throwable throwable = result.getThrowable();
        if (throwable != null) {
            writer.print("<tr><th");
            if (parameterCount > 1) {
                writer.printf(" colspan=\"%d\"", parameterCount);
            }
            writer.print(">");
            writer.print(
                (result.getStatus() == ITestResult.SUCCESS ? "Expected Exception" : "Exception"));
            writer.print("</th></tr>");

            writer.print("<tr><td");
            if (parameterCount > 1) {
                writer.printf(" colspan=\"%d\"", parameterCount);
            }
            writer.print(">");
            writeStackTrace(throwable);
            writer.print("</td></tr>");
            hasRows = true;
        }

        if (!hasRows) {
            writer.print("<tr><th");
            if (parameterCount > 1) {
                writer.printf(" colspan=\"%d\"", parameterCount);
            }
            writer.print(" class=\"invisible\"/></tr>");
        }

        writer.print("</table>");
        writer.println("<p class=\"totop\"><a href=\"#summary\">back to summary</a></p>");
    }

    private boolean dumpParametersInfo(String prefix, Object[] parameters) {
        int parameterCount = (parameters == null ? 0 : parameters.length);
        if (parameterCount == 0) {
            return false;
        }
        writer.print("<tr class=\"param\">");
        for (int i = 1; i <= parameterCount; i++) {
            writer.print(String.format("<th>%s #", prefix));
            writer.print(i);
            writer.print("</th>");
        }
        writer.print("</tr><tr class=\"param stripe\">");
        for (Object parameter : parameters) {
            writer.print("<td>");
            writer.print(Utils.escapeHtml(Utils.toString(parameter)));
            writer.print("</td>");
        }
        writer.print("</tr>");
        return true;
    }

    protected void writeReporterMessages(List<String> reporterMessages) {
        writer.print("<div class=\"messages\">");
        Iterator<String> iterator = reporterMessages.iterator();
        assert iterator.hasNext();
        if (Reporter.getEscapeHtml()) {
            writer.print(Utils.escapeHtml(iterator.next()));
        } else {
            writer.print(iterator.next());
        }
        while (iterator.hasNext()) {
            writer.print("<br/>");
            if (Reporter.getEscapeHtml()) {
                writer.print(Utils.escapeHtml(iterator.next()));
            } else {
                writer.print(iterator.next());
            }
        }
        writer.print("</div>");
    }

    protected void writeStackTrace(Throwable throwable) {
        writer.print("<div class=\"stacktrace\">");
        writer.print(Utils.shortStackTrace(throwable, true));
        writer.print("</div>");
    }

    protected void writeTableHeader(String html, String cssClasses) {
        writeTag("th", html, cssClasses);
    }

    protected void writeTableData(String html) {
        writeTableData(html, null);
    }

    protected void writeTableData(String html, String cssClasses) {
        writeTag("td", html, cssClasses);
    }

    protected void writeTag(String tag, String html, String cssClasses) {
        writer.print("<");
        writer.print(tag);
        if (cssClasses != null) {
            writer.print(" class=\"");
            writer.print(cssClasses);
            writer.print("\"");
        }
        writer.print(">");
        writer.print(html);
        writer.print("</");
        writer.print(tag);
        writer.print(">");
    }

    protected static class SuiteResult {
        private final String suiteName;
        private final List<TestResult> testResults = Lists.newArrayList();
        private final ParallelMode mode;

        public SuiteResult(ISuite suite) {
            suiteName = suite.getName();
            mode = suite.getXmlSuite().getParallel();
            for (ISuiteResult suiteResult : suite.getResults().values()) {
                testResults.add(new TestResult(suiteResult.getTestContext()));
            }
        }

        public String getSuiteName() {
            return suiteName;
        }

        public List<TestResult> getTestResults() {
            return testResults;
        }

        public ParallelMode getParallelMode() {
            return mode;
        }
    }

    protected static class TestResult {
        protected static final Comparator<ITestResult> RESULT_COMPARATOR =
            Comparator.comparing((ITestResult o) -> o.getTestClass().getName())
                .thenComparing(o -> o.getMethod().getMethodName());

        private final String testName;
        private final List<ClassResult> failedConfigurationResults;
        private final List<ClassResult> failedTestResults;
        private final List<ClassResult> skippedConfigurationResults;
        private final List<ClassResult> skippedTestResults;
        private final List<ClassResult> retriedTestResults;
        private final List<ClassResult> passedTestResults;
        private final int failedTestCount;
        private final int retriedTestCount;
        private final int skippedTestCount;
        private final int passedTestCount;
        private final long duration;
        private final String includedGroups;
        private final String excludedGroups;

        public TestResult(ITestContext context) {
            testName = context.getName();

            Set<ITestResult> failedConfigurations = context.getFailedConfigurations().getAllResults();
            Set<ITestResult> failedTests = context.getFailedTests().getAllResults();
            Set<ITestResult> skippedConfigurations = context.getSkippedConfigurations().getAllResults();
            Set<ITestResult> rawSkipped = context.getSkippedTests().getAllResults();
            Set<ITestResult> skippedTests = pruneSkipped(rawSkipped);
            Set<ITestResult> retriedTests = pruneRetried(rawSkipped);

            Set<ITestResult> passedTests = context.getPassedTests().getAllResults();

            failedConfigurationResults = groupResults(failedConfigurations);
            failedTestResults = groupResults(failedTests);
            skippedConfigurationResults = groupResults(skippedConfigurations);
            skippedTestResults = groupResults(skippedTests);
            retriedTestResults = groupResults(retriedTests);
            passedTestResults = groupResults(passedTests);

            failedTestCount = failedTests.size();
            retriedTestCount = retriedTests.size();
            skippedTestCount = skippedTests.size();
            passedTestCount = passedTests.size();

            duration = context.getEndDate().getTime() - context.getStartDate().getTime();

            includedGroups = formatGroups(context.getIncludedGroups());
            excludedGroups = formatGroups(context.getExcludedGroups());
        }

        private static Set<ITestResult> pruneSkipped(Set<ITestResult> results) {
            return results.stream().filter(result -> !result.wasRetried()).collect(Collectors.toSet());
        }

        private static Set<ITestResult> pruneRetried(Set<ITestResult> results) {
            return results.stream().filter(ITestResult::wasRetried).collect(Collectors.toSet());
        }

        protected List<ClassResult> groupResults(Set<ITestResult> results) {
            List<ClassResult> classResults = Lists.newArrayList();
            if (!results.isEmpty()) {
                List<MethodResult> resultsPerClass = Lists.newArrayList();
                List<ITestResult> resultsPerMethod = Lists.newArrayList();

                List<ITestResult> resultsList = Lists.newArrayList(results);
                resultsList.sort(RESULT_COMPARATOR);
                Iterator<ITestResult> resultsIterator = resultsList.iterator();
                assert resultsIterator.hasNext();

                ITestResult result = resultsIterator.next();
                resultsPerMethod.add(result);

                String previousClassName = result.getTestClass().getName();
                String previousMethodName = result.getMethod().getMethodName();
                while (resultsIterator.hasNext()) {
                    result = resultsIterator.next();

                    String className = result.getTestClass().getName();
                    if (!previousClassName.equals(className)) {
                        // Different class implies different method
                        assert !resultsPerMethod.isEmpty();
                        resultsPerClass.add(new MethodResult(resultsPerMethod));
                        resultsPerMethod = Lists.newArrayList();

                        assert !resultsPerClass.isEmpty();
                        classResults.add(new ClassResult(previousClassName, resultsPerClass));
                        resultsPerClass = Lists.newArrayList();

                        previousClassName = className;
                        previousMethodName = result.getMethod().getMethodName();
                    } else {
                        String methodName = result.getMethod().getMethodName();
                        if (!previousMethodName.equals(methodName)) {
                            assert !resultsPerMethod.isEmpty();
                            resultsPerClass.add(new MethodResult(resultsPerMethod));
                            resultsPerMethod = Lists.newArrayList();

                            previousMethodName = methodName;
                        }
                    }
                    resultsPerMethod.add(result);
                }
                assert !resultsPerMethod.isEmpty();
                resultsPerClass.add(new MethodResult(resultsPerMethod));
                assert !resultsPerClass.isEmpty();
                classResults.add(new ClassResult(previousClassName, resultsPerClass));
            }
            return classResults;
        }

        public String getTestName() {
            return testName;
        }

        public List<ClassResult> getFailedConfigurationResults() {
            return failedConfigurationResults;
        }

        public List<ClassResult> getFailedTestResults() {
            return failedTestResults;
        }

        public List<ClassResult> getSkippedConfigurationResults() {
            return skippedConfigurationResults;
        }

        public List<ClassResult> getSkippedTestResults() {
            return skippedTestResults;
        }

        public List<ClassResult> getRetriedTestResults() {
            return retriedTestResults;
        }

        public List<ClassResult> getPassedTestResults() {
            return passedTestResults;
        }

        public int getFailedTestCount() {
            return failedTestCount;
        }

        public int getSkippedTestCount() {
            return skippedTestCount;
        }

        public int getRetriedTestCount() {
            return retriedTestCount;
        }

        public int getPassedTestCount() {
            return passedTestCount;
        }

        public long getDuration() {
            return duration;
        }

        public String getIncludedGroups() {
            return includedGroups;
        }

        public String getExcludedGroups() {
            return excludedGroups;
        }

        protected String formatGroups(String[] groups) {
            if (groups.length == 0) {
                return "";
            }

            StringBuilder builder = new StringBuilder();
            builder.append(groups[0]);
            for (int i = 1; i < groups.length; i++) {
                builder.append(", ").append(groups[i]);
            }
            return builder.toString();
        }
    }

    protected static class ClassResult {
        private final String className;
        private final List<MethodResult> methodResults;

        public ClassResult(String className, List<MethodResult> methodResults) {
            this.className = className;
            this.methodResults = methodResults;
        }

        public String getClassName() {
            return className;
        }

        public List<MethodResult> getMethodResults() {
            return methodResults;
        }
    }

    protected static class MethodResult {
        private final List<ITestResult> results;

        public MethodResult(List<ITestResult> results) {
            this.results = results;
        }

        public List<ITestResult> getResults() {
            return results;
        }
    }
}

