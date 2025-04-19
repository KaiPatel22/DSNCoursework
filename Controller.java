import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class Controller {
    private int cport;
    private int replicationFactor;
    private int timeout;
    private int rebalance_period;
    static Index index;
    private ArrayList<Integer> dstorePortsConnected; // Keeps track of dstore ports connected.
    private final Map<String, CountDownLatch> filenameCountdownMap = Collections.synchronizedMap(new HashMap<>()); // Map to link the filename and the countdown before the STORE_ACK timeouts.

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
            }else if (message.startsWith("STORE ")) {
                handleStoreOperation(controllerSocket, message);
            }else if (message.startsWith("STORE_ACK ")){
                handleSTORE_ACK(message);
            }else{
                System.err.println("ERROR: Invalid message format.");
                return;
            }
        }catch(Exception e){
            System.err.println("ERROR: Could not read message from controller socket: " + e);
        }
    }

    public void handleJoinOperation(String message){
        System.out.println("JOIN message recieved!");
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
        System.out.println("STORE message recieved!");
        String[] parts = message.split(" ");
        if (parts.length < 3){
            System.err.println("ERROR: Invalid STORE message format.");
        }
        String filename = parts[1];
        long filesize = Long.parseLong(parts[2]);
        if (dstorePortsConnected.size() < replicationFactor){ // NOT ENOUGH DSTORES CHECK
            try{
                PrintWriter out = new PrintWriter(controllerSocket.getOutputStream(), true);
                out.println("ERROR_NOT_ENOUGH_DSTORES");
                System.err.println("ERROR: Not enough Dstores connected to store " + filename);
            }catch (Exception e){
                System.err.println("ERROR: Could not send NOT_ENOUGH_DSTORES to controller socket: " + e);
            }
            return;
        }
        if (index.getFileInformation(filename) != null){ // FILE ALREADY EXISTS CHECK
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

            CountDownLatch latch = new CountDownLatch(selectedDstores.size());
            filenameCountdownMap.put(filename, latch);
            try{
                boolean allAcksRecieved = latch.await(timeout, TimeUnit.MILLISECONDS);
                if (allAcksRecieved) {
                    index.updateFileStatus(filename, Index.FileStatus.STORE_COMPLETE);
                    out.println("STORE_COMPLETE");
                    System.out.println("File " + filename + " stored successfully on Dstores: " + selectedDstores);
                }else{
                    System.err.println("ERROR: Timeout waiting for STORE_ACK for file " + filename);
                    index.removeFile(filename);
                }
            }catch (Exception e){
                System.err.println("ERROR: Could not wait for STORE_ACK: " + e);
            }finally {
                filenameCountdownMap.remove(filename);
            }

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

    public void handleSTORE_ACK(String message){
        System.out.println("STORE_ACK message recieved!");
        String[] parts = message.split(" ");
        if (parts.length < 2){
            System.err.println("ERROR: Invalid STORE_ACK message");
        }
        String filename = parts[1];
        CountDownLatch latch = filenameCountdownMap.get(filename);
        if (latch != null){
            latch.countDown();
            System.out.println("Received STORE_ACK for file " + filename);
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
