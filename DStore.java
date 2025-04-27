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
            sendStoreMessage("supercoolfile", 123); // Store message, remove message from here later (only for testing)
            sendStore_Ack("supercoolfile");
        } catch (IOException e) {
            System.err.println("ERROR: Connecting to controller was not performed: " + e.getMessage());
        }
    }

    public void handleOperation(Socket dstoreSocket){
        try{
            BufferedReader reader = new BufferedReader(new InputStreamReader(dstoreSocket.getInputStream()));
            String message = reader.readLine();
            if (message.startsWith("STORE ")){
                handleStoreOperation(dstoreSocket, message);
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
        if (parts.length < 3){
            System.err.println("ERROR: Invalid STORE message format.");
        }
        String filename = parts[1];
        long filesize = Long.parseLong(parts[2]);

        try{
            PrintWriter out = new PrintWriter(dStoreSocket.getOutputStream(), true);
            out.println("ACK");
            System.out.println("Sent ACK message to client");

            InputStream inputStream = dStoreSocket.getInputStream();
            byte[] fileBytes = new byte[(int)filesize];
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
            sendStore_Ack(filename);
        }catch(Exception e){
            System.err.println("ERROR: Could not handle STORE operation: " + e.getMessage());
        }
    }

    private void sendStore_Ack(String filename){
        try{
            PrintWriter out = new PrintWriter(controllerSocket.getOutputStream(), true);
            out.println("STORE_ACK " + filename);
            System.out.println("Sent STORE_ACK message for " + filename + " to controller!");
        }catch(Exception e){
            System.err.println("ERROR: Could not send STORE_ACK message: " + e.getMessage());
        }
    }

    private void sendJoinMessage() throws IOException {
        PrintWriter out = new PrintWriter(controllerSocket.getOutputStream(), true);
        out.println("JOIN " + port);
        System.out.println("Sent JOIN message to controller");
    }

    private void sendStoreMessage(String fileName, long filesize) throws IOException { // Only for testing remove later, Client sends STORE message
        PrintWriter out = new PrintWriter(controllerSocket.getOutputStream(), true);
        out.println("STORE " + fileName + " " + filesize);
        System.out.println("Sent STORE message to controller");
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
