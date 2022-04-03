
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.io.*;

public class MyClient {

  public static String HELO = "HELO";
  public static String AUTH = "AUTH " + System.getProperty("user.name");
  public static String REDY = "REDY";
  public static String OK = "OK";
  public static String GETSALL = "GETS All";
  public static String QUIT = "QUIT";
  public static String JOBN = "JOBN";
  public static String NONE = "NONE";

  public MyClient() {
  }

  public String readMsg(byte[] b, BufferedInputStream bis) {
    try {
      bis.read(b);
      String str = new String(b, StandardCharsets.UTF_8);
      return str;
    } catch (Exception e) {
      System.out.println(e);
    }
    return "error";
  }

  public static void main(String args[]) throws Exception {
    try {

      Socket socket = new Socket("localhost", 50000);

      DataInputStream din = new DataInputStream(socket.getInputStream());
      DataOutputStream dout = new DataOutputStream(socket.getOutputStream());
      BufferedInputStream bin = new BufferedInputStream(din);
      BufferedOutputStream bout = new BufferedOutputStream(dout);

      MyClient myClient = new MyClient();

      Boolean isJobLeft = true;

      String largestServer = null;

      bout.write(HELO.getBytes());

      bout.flush();

      String serverResponse = myClient.readMsg(new byte[2], bin);

      bout.write(AUTH.getBytes());
      bout.flush();

      serverResponse = myClient.readMsg(new byte[2], bin);

      bout.write(REDY.getBytes());
      bout.flush();

      while (isJobLeft) {

        serverResponse = myClient.readMsg(new byte[64], bin);

        if (serverResponse.substring(0, 4).equals(NONE) || serverResponse.substring(0, 4).equals(QUIT)) {
          isJobLeft = false;
          bout.write(QUIT.getBytes());
          bout.flush();
          break;
        }

        if (!(serverResponse.substring(0, 4).equals(JOBN))) {
          bout.write(REDY.getBytes());
          bout.flush();
          continue;
        }

        else {

          String[] JOBNSplit = serverResponse.split(" ");

          int JobID = Integer.parseInt(JOBNSplit[2]);

          bout.write(GETSALL.getBytes());
          bout.flush();

          serverResponse = myClient.readMsg(new byte[32], bin);

          bout.write(OK.getBytes());
          bout.flush();

          String[] message_space = serverResponse.split(" ");

          String str = new String();

          for (int i = 0; i < message_space[2].length(); i++) {
            char c = message_space[2].charAt(i);
            if (c == '0' || c == '1' || c == '2' || c == '3' || c == '4' || c == '5' || c == '6' || c == '7' || c == '8'
                || c == '9')
              str += c;
            else
              break;
          }

          serverResponse = myClient.readMsg(new byte[Integer.parseInt(message_space[1]) * Integer.parseInt(str)], bin);

          String[] arrOfStr = serverResponse.split("\n");
          if (largestServer == null) {

            String biggestServer = arrOfStr[0];

            for (int i = 0; i < arrOfStr.length; i++) {

              String[] serverInfo = arrOfStr[i].split(" ");

              String[] BigSplitInfo = biggestServer.split(" ");

              int currentCore = Integer.parseInt(serverInfo[4]);
              int biggestCore = Integer.parseInt(BigSplitInfo[4]);

              if (currentCore > biggestCore) {
                biggestServer = arrOfStr[i];
              }
            }

            String[] bigSplit = biggestServer.split("\\s+");
            largestServer = bigSplit[0];
          }

          bout.write(OK.getBytes());
          bout.flush();

          serverResponse = myClient.readMsg(new byte[1], bin);

          String SCHD = "SCHD" + " " + JobID + " " + largestServer + " " + "0";
          bout.write(SCHD.getBytes());
          bout.flush();

          System.out.println("The biggest server is: " + largestServer);

          serverResponse = myClient.readMsg(new byte[2], bin);
          System.out.println("SCHD response: " + serverResponse);

          bout.write(REDY.getBytes());
          bout.flush();
        }
      }

      serverResponse = myClient.readMsg(new byte[32], bin);

      if (serverResponse.equals(QUIT)) {
        bout.close();
        dout.close();
        bin.close();
        din.close();
        socket.close();
      }
    }

    catch (Exception e) {
      System.out.println(e);
    }
  }
}
