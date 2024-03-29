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

package utils;

import jakarta.mail.Authenticator;
import jakarta.mail.Message;
import jakarta.mail.MessagingException;
import jakarta.mail.Multipart;
import jakarta.mail.PasswordAuthentication;
import jakarta.mail.Session;
import jakarta.mail.Transport;
import jakarta.mail.internet.InternetAddress;
import jakarta.mail.internet.MimeBodyPart;
import jakarta.mail.internet.MimeMessage;
import jakarta.mail.internet.MimeMultipart;
import myobject.MailObject;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

public class SendEmailClient {
    private Session session;
    private Date date = new Date();

    public static class SendEmailClientBuilder {
        private String serverHost = "smtp.partner.outlook.cn";
        private int serverPort = 587;
        private String serverUsername = System.getenv("EMAILUSER");
        private String serverPassword = System.getenv("EMAILPASS");
        private boolean useStarttls = true;
        private Session session;

        public SendEmailClientBuilder serverHost(String serverHost) {
            this.serverHost = serverHost;
            return this;
        }

        public SendEmailClientBuilder serverPort(int serverPort) {
            this.serverPort = serverPort;
            return this;
        }

        public SendEmailClientBuilder serverUsername(String serverUsername) {
            this.serverUsername = serverUsername;
            return this;
        }

        public SendEmailClientBuilder serverPassword(String serverPassword) {
            this.serverPassword = serverPassword;
            return this;
        }

        public SendEmailClientBuilder useStarttls(boolean useStarttls) {
            this.useStarttls = useStarttls;
            return this;
        }

        public SendEmailClient build() {
            Properties properties = System.getProperties();
            properties.setProperty("mail.smtp.host", serverHost);
            properties.setProperty("mail.smtp.port", String.valueOf(serverPort));
            if (useStarttls) {
                properties.put("mail.smtp.starttls.enable", "true");
            }
            properties.put("mail.smtp.auth", "true");
            session = Session.getDefaultInstance(properties, new Authenticator() {
                @Override
                protected PasswordAuthentication getPasswordAuthentication() {
                    return new PasswordAuthentication(serverUsername, serverPassword);
                }
            });

            return new SendEmailClient(this);
        }
    }

    private SendEmailClient(SendEmailClientBuilder sendEmailClientBuilder) {
        session = sendEmailClientBuilder.session;
    }

    public void sendHTMLEmail(String htmlPath) throws MessagingException, IOException {
        MailObject mailObject = new MailObject();
        mailObject.setFrom(System.getenv("EMAILFROM"));
        mailObject.setCc(new String[]{System.getenv("EMAILCC")});
        mailObject.setTo(new String[]{System.getenv("EMAILTO")});
        String str = "-yyyyMMddHHmmss";
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(str);
        mailObject.setSubject("DingoDB每日测试报告" + simpleDateFormat.format(date));
        mailObject.setBody("测试报告");
        mailObject.setFiles(new File[] {new File(htmlPath)});

        Message message = new MimeMessage(session);

        message.setFrom(new InternetAddress(mailObject.getFrom()));

        InternetAddress[] addressTo = new InternetAddress[mailObject.getTo().length];
        for (int i = 0, j = mailObject.getTo().length; i < j; i++) {
            addressTo[i] = new InternetAddress(mailObject.getTo()[i]);
        }

        //message.addRecipient(Message.RecipientType.TO, new InternetAddress(to));
        message.setRecipients(Message.RecipientType.TO, addressTo);

        InternetAddress[] addressCc = new InternetAddress[mailObject.getCc().length];
        for (int i = 0, j = mailObject.getCc().length; i < j; i++) {
            addressCc[i] = new InternetAddress(mailObject.getCc()[i]);
        }

        message.setRecipients(Message.RecipientType.CC, addressCc);

        message.setSubject(mailObject.getSubject());

        Multipart mp = new MimeMultipart();

        MimeBodyPart body = new MimeBodyPart();
        body.setContent(readFile(new File(htmlPath)), "text/html;charset=utf-8");

        mp.addBodyPart(body);

        //附件
        for (File file : mailObject.getFiles()) {
            MimeBodyPart attachment = new MimeBodyPart();
            attachment.attachFile(file);
            mp.addBodyPart(attachment);
        }

        message.setContent(mp);
        Transport.send(message);
    }

    private String readFile(File file) {
        FileReader fileReader = null;
        StringBuilder stringBuilder = null;
        try {
            fileReader = new FileReader(file);
            char[] chars = new char[1024];
            int len;
            stringBuilder = new StringBuilder();
            while ((len = fileReader.read(chars)) != -1) {
                for (int i = 0; i < len; i++) {
                    stringBuilder.append(chars[i]);
                }
//                for (char tmp : chars) {
//                    stringBuilder.append(tmp);
//                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (fileReader != null) {
                try {
                    fileReader.close();
                } catch (IOException e) {
                    return "";
                }
            }
            return stringBuilder.toString();
        }
    }

}
