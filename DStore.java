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
                    Socket dstoreClient = dstoreServerSocket.accept();
                    System.out.println("Connected Successfully on port " + dstoreClient.getPort() + " with address " + dstoreClient.getInetAddress());
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
        } catch (IOException e) {
            System.err.println("Error connecting to controller: " + e.getMessage());
        }
    }

    private void sendJoinMessage() throws IOException {
        PrintWriter out = new PrintWriter(controllerSocket.getOutputStream(), true);
        out.println("JOIN " + port);
        System.out.println("Sent JOIN message to controller");
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
