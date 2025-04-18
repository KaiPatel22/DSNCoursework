import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.*;
import java.util.ArrayList;
import java.util.Collections;

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
                            handleOperation(controllerSocket);
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

    public void handleOperation(Socket controllerSocket){
        try{
            BufferedReader reader = new BufferedReader(new InputStreamReader(controllerSocket.getInputStream()));
            String message = reader.readLine();
            if (message.startsWith("JOIN ")){
                handleJoinOperation(message);
            }else if (message.startsWith("STORE ")){
                handleStoreOperation(controllerSocket, message);
            }else{
                return;
            }
        }catch(Exception e){
            System.err.println("ERROR: Could not read message from controller socket: " + e);
        }
    }

    public void handleJoinOperation(String message){
        String[] parts = message.split(" ");
        int dstorePort = Integer.parseInt(parts[1]);
        if (!dstorePortsConnected.contains(dstorePort)) {
            dstorePortsConnected.add(dstorePort);
            System.out.println("Added Dstore port " + dstorePort + " to connected list.");
            System.out.println("List of Dstore ports connected: " + dstorePortsConnected);
        }else{
            System.out.println("Dstore port " + dstorePort + " is already connected.");
            System.out.println("List of Dstore ports connected: " + dstorePortsConnected);
        }
    }

    public void handleStoreOperation(Socket controllerSocket, String message){
        String[] parts = message.split(" ");
        if (parts.length < 3){
            System.err.println("ERROR: Invalid STORE message format.");
        }
        String filename = parts[1];
        long filesize = Long.parseLong(parts[2]);
        if (dstorePortsConnected.size() < replicationFactor){
            try{
                PrintWriter out = new PrintWriter(controllerSocket.getOutputStream(), true);
                out.println("ERROR_NOT_ENOUGH_DSTORES");
                System.err.println("ERROR: Not enough Dstores connected to store " + filename);
            }catch (Exception e){
                System.err.println("ERROR: Could not send NOT_ENOUGH_DSTORES to controller socket: " + e);
            }
        }
        if (index.getFileInformation(filename) != null){
            try{
                PrintWriter out = new PrintWriter(controllerSocket.getOutputStream(), true);
                out.println("ERROR_FILE_ALREADY_EXISTS");
                System.err.println("ERROR: File " + filename + " already exists.");
            } catch (Exception e) {
                System.err.println("ERROR: Could not send FILE_ALREADY_EXISTS to controller socket: " + e);
            }
        }
        ArrayList<Integer> selectedDstores = selectDstores();
        index.addFile(filename, filesize, selectedDstores);
        try{
            PrintWriter out = new PrintWriter(controllerSocket.getOutputStream(), true);
            StringBuilder response = new StringBuilder("STORE_TO");
            for (Integer port : selectedDstores) {
                response.append(" ").append(port);
            }
            out.println(response);
            // Add code here for waiting for STORE_ACK from each DStore, below it is for an immediate ACK
            index.updateFileStatus(filename, Index.FileStatus.STORE_COMPLETE);
            out.println("STORE_COMPLETE");
        } catch (Exception e) {
            System.err.println("ERROR: HELP");
        }

    }

    private ArrayList<Integer> selectDstores() { //Ensure files are distributed evenly among Dstores
        ArrayList<Integer> sorted = new ArrayList<>(dstorePortsConnected);
        Collections.sort(sorted);
        ArrayList<Integer> selected = new ArrayList<>();
        for (int i = 0; i < replicationFactor; i++) {
            selected.add(sorted.get(i));
        }
        return selected;
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
