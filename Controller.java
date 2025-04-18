import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.*;
import java.util.ArrayList;

public class Controller {
    private int cport;
    private int replicationFactor;
    private int timeout;
    private int rebalance_period;
    static Index index; // Instance object of index, final because it doesn't differ between controller objects
    private ArrayList<Integer> dstorePortsConnected; // This differs from what the index stores, index only keeps track of files but this list keeps track of dstore ports connected.

    public Controller(int cport, int replicationFactor, int timeout, int rebalance_period) {
        this.cport = cport;
        this.replicationFactor = replicationFactor;
        this.timeout = timeout;
        this.rebalance_period = rebalance_period;
        index = new Index();
        dstorePortsConnected = new ArrayList<Integer>();

    }

    public void start(){
        ServerSocket controllerServerSocket;
        try{
            controllerServerSocket = new ServerSocket(cport);
            System.out.println("Controller started on port " + cport);
            for(;;){
                try{
                    System.out.println("Waiting for connection");
                    Socket controllerSocket = controllerServerSocket.accept();
                    System.out.println("Connected Successfully on port " + controllerSocket.getPort() + " with address " + controllerSocket.getInetAddress());
                    new Thread(new Runnable() {
                        @Override
                        public void run() {
                            handleConnection(controllerSocket);
                        }
                    }).start();
                }catch(Exception e){
                    System.err.println("ERROR: Could not accept connection: " + e);
                }
            }
        }catch(Exception e){
            System.err.println("ERROR: Could not create controller socket: " + e);
        }
    }

    public void handleConnection(Socket controllerSocket){
        try{
            BufferedReader reader = new BufferedReader(new InputStreamReader(controllerSocket.getInputStream()));
            String message = reader.readLine();
            if (message.startsWith("JOIN")) {
                String[] parts = message.split(" ");
                int dstorePort = Integer.parseInt(parts[1]);
                if (!dstorePortsConnected.contains(dstorePort)) {
                    dstorePortsConnected.add(dstorePort);
                    System.out.println("Added Dstore port " + dstorePort + " to connected list.");
                }else{
                    System.out.println("Dstore port " + dstorePort + " is already connected.");
                }
            }
        }catch(Exception e){
            System.err.println("ERROR: Could not read message from controller socket: " + e);
        }
    }

    public static void main(String[] args) {
        int cport = Integer.parseInt(args[0]);
        int replicationFactor = Integer.parseInt(args[1]);
        int timeout = Integer.parseInt(args[2]);
        int rebalance_period = Integer.parseInt(args[3]);

        Controller controller = new Controller(cport, replicationFactor, timeout, rebalance_period);
        controller.start();
    }
}
