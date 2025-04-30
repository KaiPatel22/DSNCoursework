import java.io.*;
import java.net.*;

public class DStore {

    private int port;
    private int cport;
    private int timeout;
    private String file_folder;
    Socket controllerSocket;


    public DStore(int port, int cport, int timeout, String file_folder) {
        this.port = port;
        this.cport = cport;
        this.timeout = timeout;
        this.file_folder = file_folder;
    }

    public void start(){
        ServerSocket dstoreServerSocket;
        try{
            dstoreServerSocket = new ServerSocket(port);
            connectToController();
            for(;;){
                try{
                    System.out.println("Waiting for connection");
                    Socket dstoreSocket = dstoreServerSocket.accept();
                    System.out.println("Connected Successfully on port " + dstoreSocket.getPort() + " with address " + dstoreSocket.getInetAddress());
                    new Thread(new Runnable() {
                        @Override
                        public void run() {
                            handleOperation(dstoreSocket);
                        }
                    }).start();
                }catch (Exception e) {
                    System.err.println("ERROR: Could not accept connection: " + e);
                }
            }
        }catch (Exception e){
            System.err.println("ERROR: Could not create DStore socket: " + e);
        }
    }

    private void connectToController() {
        try {
            controllerSocket = new Socket(InetAddress.getLocalHost(), cport);
            System.out.println("Connected to controller on port " + controllerSocket.getPort() + " with address " + controllerSocket.getInetAddress());
            sendJoinMessage(); // Join message to controller
        } catch (Exception e) {
            System.err.println("ERROR: Connecting to controller was not performed: " + e.getMessage());
        }
    }

    private void sendJoinMessage() {
        sendMessage(controllerSocket, "JOIN " + port);
        System.out.println("Sent JOIN message to controller");
    }

    private void handleOperation(Socket dstoreSocket){
        try{
            BufferedReader reader = new BufferedReader(new InputStreamReader(dstoreSocket.getInputStream()));
            String message = reader.readLine();
            if (message.startsWith("STORE ")) {
                handleStoreOperation(dstoreSocket, message);
            }else if (message.startsWith("LOAD_DATA ")) {
                handleLoad_DataOperation(dstoreSocket, message);
            }else if (message.startsWith("REMOVE ")) {
                handleRemoveOperation(message);
            } else {
                System.err.println("ERROR: Invalid message format.");
            }
        }catch(Exception e){
            System.err.println("ERROR: Could not handle operation: " + e.getMessage());
        }
    }

    private void handleStoreOperation(Socket dStoreSocket, String message){
        System.out.println("STORE message received by DStore!");
        String[] parts = message.split(" ");
        if (parts.length != 3){
            System.err.println("ERROR: Malformed STORE message");
        }
        String filename = parts[1];
        long filesize = Long.parseLong(parts[2]);

        try{
            sendMessage(dStoreSocket, "ACK");
            System.out.println("Sent ACK message to client");

            InputStream inputStream = dStoreSocket.getInputStream();
            byte[] fileBytes = inputStream.readNBytes((int) filesize);
            int totalRead = 0;
            while(totalRead < filesize){
                int read = inputStream.read(fileBytes, totalRead, (int)(filesize - totalRead));
                if (read == -1){
                    break;
                }
                totalRead += read;
            }
            File file = new File(file_folder, filename);
            try(FileOutputStream fileOutputStream = new FileOutputStream(file)){
                fileOutputStream.write(fileBytes);
            }
            System.out.println("File " + filename + " stored in DStore!");

            sendMessage(controllerSocket, "STORE_ACK " + filename);
            System.out.println("Sent STORE_ACK message for " + filename + " to controller!");
        }catch(Exception e){
            System.err.println("ERROR: Could not handle STORE operation: " + e.getMessage());
        }
    }


    private void handleLoad_DataOperation(Socket dStoreSocket, String message){
        System.out.println("LOAD_DATA message received!");
        String[] parts = message.split(" ");
        if (parts.length != 2){
            System.err.println("ERROR: Malformed LOAD_DATA message");
        }
        String filename = parts[1];
        File file = new File(file_folder, filename);
        if (!file.exists()){
            System.err.println("ERROR: File " + filename + " does not exist in DStore");
            try{
                dStoreSocket.close();
            }catch (Exception e){
                System.err.println("ERROR: Could not close socket: " + e.getMessage());
            }
            return;
        }
        try {
            FileInputStream fileInputStream = new FileInputStream(file);
            BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(dStoreSocket.getOutputStream());
            byte[] buffer = new byte[4096];
            int bytesRead;
            while ((bytesRead = fileInputStream.read(buffer)) != -1) {
                bufferedOutputStream.write(buffer, 0, bytesRead);
            }
            bufferedOutputStream.flush();
            System.out.println("Contents of " + filename + " sent to client!");
        }catch (Exception e){
            System.err.println("ERROR: Could not handle LOAD_DATA operation: " + e.getMessage());
        }
    }

    private void handleRemoveOperation(String message){
        System.out.println("REMOVE message received!");
        String[] parts = message.split(" ");
        if (parts.length != 2){
            System.err.println("ERROR: Malformed REMOVE message");
        }
        String filename = parts[1];
        File file = new File(file_folder, filename);
        if (!file.exists()){
            System.err.println("ERROR: File " + filename + " does not exist in DStore");
            sendMessage(controllerSocket, "ERROR_FILE_DOES_NOT_EXIST " + filename);
            return;
        }else{
            try{
                file.delete();
                System.out.println("File " + filename + " deleted from DStore!");
                sendMessage(controllerSocket, "REMOVE_ACK " + filename);
                System.out.println("Sent REMOVE_ACK message for " + filename + " to controller!");
            }catch(Exception e){
                System.err.println("ERROR: Could not delete file " + filename + ": " + e.getMessage());
            }
        }
    }

    private void sendMessage(Socket socket, String message){
        try {
            PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
            out.println(message);
            System.err.println(message);
        } catch (Exception e) {
            System.err.println("ERROR: Could not send message: " + message + "\n" + e);
        }
    }

    public static void main(String[] args) {
        int port = Integer.parseInt(args[0]);
        int cport = Integer.parseInt(args[1]);
        int timeout = Integer.parseInt(args[2]);
        String file_folder = args[3];

        DStore dstore = new DStore(port, cport, timeout, file_folder);
        dstore.start();
    }
}
