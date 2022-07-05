# dingo-auto-tests
Auto tests for DingoDB

**Run in IntelliJ IDEA in Windows OS**

prerequisite:
1. Install JDK 8.x in windows.
2. Add system environment variable

    . name: ConnectIP    value: [your DingoDB driver IP address]

    . name: EMAILFROM    value: [your send email address]

    . name: EMAILTO      value: [your receive email address] 

    . name: EMAILCC      value: [your cc receive email address]

    . name: EMAILUSER    value: [your send email user]

    . name: EMAILPASS    value: [your send email password]

steps:
1. clone this repository of dev branch to your windows machine. 
2. Open this project using InteliJ IDEA.
3. Execute Gradle task:
    _gradle test_


**Run in LinuxOS**

prerequisite same with Windows OS.

In addition, one more prerequisite is: 
install gradle(v 7.3.3) in linux OS and add gradle /bin to path environment variable.Such as
_export PATH=$PATH:/opt/gradle/gradle-7.3.3/bin_

steps:
1. clone this repository of dev branch to your linux host.
2. Navigate to dingo-auto-tests/dingo-testng directory.
3. Run command:
    _gradle clean test_

**Report view**

After gradle test task run finished, you can find the report in directory dingo-auto-tests/dingo-testng/ReportOut/[RunDateFolder]/xxx.html

If you configuration of email works fine, you can also see the email with html report attachment send to your email box.