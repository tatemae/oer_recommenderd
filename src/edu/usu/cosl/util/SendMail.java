package edu.usu.cosl.util;


import javax.mail.*;
import javax.mail.internet.*;
import java.util.*;

public class SendMail
{
	public static void sendMsg(String sSMTPServer, String sFrom, String sTo, String sSubject, String sBody)
	{
		try
		{
			Properties props = System.getProperties();

			// -- Attaching to default Session, or we could start a new one --
			props.put("mail.smtp.host", sSMTPServer);
			Session smtpSession = Session.getDefaultInstance(props, null);

			// -- Create a new message --
			Message msg = new MimeMessage(smtpSession);

			// -- Set the FROM and TO fields --
			msg.setFrom(new InternetAddress(sFrom));
			msg.setRecipients(Message.RecipientType.TO, InternetAddress.parse(sTo, false));

			// -- We could include CC recipients too --
			// if (cc != null) msg.setRecipients(Message.RecipientType.CC ,InternetAddress.parse(cc, false));

			// -- Set the subject and body text --
			msg.setSubject(sSubject);
			msg.setText(sBody);

			// -- Set some other header information --
			msg.setHeader("X-Mailer", "OER Recommender");
			msg.setSentDate(new Date());

			// -- Send the message --
			Transport.send(msg);
			//System.out.println("Message sent OK.");
		}
		catch (Exception ex) { System.out.println(ex);ex.printStackTrace(); }
	}
	public static void main(String[] args)
	{
		sendMsg("localhost", "oerrecommender@cosl.usu.edu", args[0], "Test Email", "That is it folks");
	}
}
