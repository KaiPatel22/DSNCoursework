import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class Controller {
    private int cport;
    private int replicationFactor;
    private int timeout;
    private int rebalance_period;
    static Index index;
    private final Map<String, CountDownLatch> filenameCountdownMap = Collections.synchronizedMap(new HashMap<>()); // Map to link the filename and the countdown before the STORE_ACK timeouts.
    private ConcurrentHashMap<Socket, Integer> clientLastUsedPortMap = new ConcurrentHashMap<>();
    private final Map<String, CountDownLatch> removeCountdownMap = Collections.synchronizedMap(new HashMap<>());
    private Map<Integer, Socket> dstorePortsSocketsMap = new ConcurrentHashMap<>();

    public Controller(int cport, int replicationFactor, int timeout, int rebalance_period) {
        this.cport = cport;
        this.replicationFactor = replicationFactor;
        this.timeout = timeout;
        this.rebalance_period = rebalance_period;
        index = new Index();
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

    private void handleOperation(Socket controllerSocket){
        try{
            BufferedReader reader = new BufferedReader(new InputStreamReader(controllerSocket.getInputStream()));
            String message;
            while ((message = reader.readLine()) != null) {
                if (message.startsWith("JOIN ")){
                    handleJoinOperation(message);
                }else if (message.startsWith("STORE ")) {
                    handleStoreOperation(controllerSocket, message);
                }else if (message.startsWith("STORE_ACK ")) {
                    handleSTORE_ACK(message);
                }else if (message.startsWith("LOAD ")) {
                    handleLoadOperation(controllerSocket, message);
                }else if (message.startsWith("RELOAD ")) {
                    handleReloadOperation(controllerSocket, message);
                }else if (message.startsWith("REMOVE ")) {
                    handleRemoveOperation(controllerSocket, message);
                }else if (message.startsWith("REMOVE_ACK ")) {
                    handleREMOVE_ACK(message);
                }else if (message.equals("LIST")) {
                    handleListOperation(controllerSocket, message);
                }else{
                    System.err.println("ERROR: Invalid message format");
                    return;
                }
            }
        }catch(Exception e){
            System.err.println("ERROR: Could not read message from controller socket: " + e);
        }
    }

    private void handleJoinOperation(String message) {
        System.out.println("JOIN message recieved!");
        String[] parts = message.split(" ");
        int dstorePort = Integer.parseInt(parts[1]);
        try{
            if (!dstorePortsSocketsMap.containsKey(dstorePort)){
                Socket dstoreSocket = new Socket(InetAddress.getLocalHost(), dstorePort);
                dstorePortsSocketsMap.put(dstorePort, dstoreSocket);
                System.out.println("Added Dstore port " + dstorePort + " to connected list.");
                System.out.println("List of Dstore ports connected: " + dstorePortsSocketsMap.keySet());
            }else{
                System.out.println("Dstore port " + dstorePort + " is already connected.");
                System.out.println("List of Dstore ports connected: " + dstorePortsSocketsMap.keySet());
            }
        }catch (Exception e){
            System.err.println("ERROR: Could not connect to Dstore socket " + dstorePort + ": " + e);
            return;
        }
    }

    private void handleStoreOperation(Socket controllerSocket, String message){
        System.out.println("STORE message recieved by Controller!");
        String[] parts = message.split(" ");
        if (parts.length != 3){
            System.err.println("ERROR: Malformed STORE message");
        }
        String filename = parts[1];
        long filesize = Long.parseLong(parts[2]);
        if (dstorePortsSocketsMap.size() < replicationFactor){
            sendMessage(controllerSocket, "ERROR_NOT_ENOUGH_DSTORES");
            System.err.println("ERROR: Not enough Dstores connected to store " + filename);
            return;
        }
        if (index.getFileInformation(filename) != null){ // FILE ALREADY EXISTS CHECK
            sendMessage(controllerSocket, "ERROR_FILE_ALREADY_EXISTS");
            System.err.println("ERROR: File " + filename + " already exists.");
            return;
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
            new Thread(() -> {
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
            }).start();
        } catch (Exception e) {
            System.err.println("ERROR: Could not send STORE_TO message to controller socket: " + e);
        }

    }

    private ArrayList<Integer> selectDstores() {
        HashMap<Integer, Integer> dstorePortsFilesStoredMap = new HashMap<>();
        for (int port : dstorePortsSocketsMap.keySet()) {
            dstorePortsFilesStoredMap.put(port, 0);
        }
        for (Map.Entry<String, Index.FileInformation> entry : index.getFileIndex().entrySet()) {
            for (int port : entry.getValue().getStoragePorts()) {
                dstorePortsFilesStoredMap.put(port, dstorePortsFilesStoredMap.get(port) + 1);
            }
        }
        ArrayList<Map.Entry<Integer, Integer>> filesStoreByPortsList = new ArrayList<>(dstorePortsFilesStoredMap.entrySet());
        filesStoreByPortsList.sort(Map.Entry.comparingByValue());

        ArrayList<Integer> selected = new ArrayList<>();
        for (int i = 0; i < replicationFactor; i++) {
            selected.add(filesStoreByPortsList.get(i).getKey());
        }
        System.out.println("The " + replicationFactor + " Dstores selected to store are: " + selected);
        return selected;
    }

    private void handleSTORE_ACK(String message){
        System.out.println("STORE_ACK message recieved!");
        String[] parts = message.split(" ");
        if (parts.length != 2){
            System.err.println("ERROR: Malformed STORE_ACK message");
        }
        String filename = parts[1];
        CountDownLatch latch = filenameCountdownMap.get(filename);
        if (latch != null){
            latch.countDown();
            System.out.println("Received STORE_ACK for file " + filename);
        }
    }

    private void handleLoadOperation(Socket controllerSocket, String message){
        System.out.println("LOAD message recieved!");
        String[] parts = message.split(" ");
        if (parts.length != 2){
            System.err.println("ERROR: Malformed LOAD message");
        }
        String filename = parts[1];
        Index.FileInformation fileInformation = index.getFileInformation(filename);
        if (fileInformation == null || fileInformation.getStatus() != Index.FileStatus.STORE_COMPLETE){ // FILE DOES NOT EXIST CHECK
            sendMessage(controllerSocket, "ERROR_FILE_DOES_NOT_EXIST");
            System.err.println("ERROR: File " + filename + " does not exist");
            return;
        }
        if (dstorePortsSocketsMap.size() < replicationFactor){
            sendMessage(controllerSocket, "ERROR_NOT_ENOUGH_DSTORES");
            System.err.println("ERROR: Not enough DStores to load " + filename);
            return;
        }
        ArrayList<Integer> listOfPorts = fileInformation.getStoragePorts();
        int chosenPort = listOfPorts.get(0);
        clientLastUsedPortMap.put(controllerSocket, chosenPort);
        long fileSize = fileInformation.getFileSize();
        sendMessage(controllerSocket, "LOAD_FROM " + chosenPort + " " + fileSize);
        System.out.println("Sent LOAD_FROM " + chosenPort + " " + fileSize + " to client");
    }

    private void handleReloadOperation(Socket controllerSocket, String message){
        System.out.println("RELOAD message recieved!");
        String[] parts = message.split(" ");
        if (parts.length != 2){
            System.err.println("ERROR: Malformed RELOAD message");
        }
        String filename = parts[1];
        Index.FileInformation fileInformation = index.getFileInformation(filename);
        if (fileInformation == null) {
            sendMessage(controllerSocket, "ERROR_FILE_DOES_NOT_EXIST");
            System.err.println("ERROR: File " + filename + " does not exist");
            return;
        }

        ArrayList<Integer> availableDstores = new ArrayList<>(fileInformation.getStoragePorts());
        Integer lastUsedPort = clientLastUsedPortMap.get(controllerSocket);
        if (lastUsedPort != null) {
            availableDstores.remove(lastUsedPort);
        }
        if (availableDstores.isEmpty()) {
            sendMessage(controllerSocket, "ERROR_LOAD");
            System.err.println("ERROR: No alternative Dstore available for reload of file " + filename);
            return;
        }
        int chosenPort = availableDstores.get(0);
        clientLastUsedPortMap.put(controllerSocket, chosenPort);
        long fileSize = fileInformation.getFileSize();

        sendMessage(controllerSocket, "LOAD_FROM " + chosenPort + " " + fileSize);
        System.out.println("Sent LOAD_FROM " + chosenPort + " " + fileSize + " to client");
    }

    private void handleRemoveOperation(Socket controllerSocket, String message){
        System.out.println("REMOVE message recieved!");
        String[] parts = message.split(" ");
        if (parts.length != 2){
            System.err.println("ERROR: Malformed REMOVE message");
        }
        String filename = parts[1];

        Index.FileInformation fileInformation = index.getFileInformation(filename);
        if (fileInformation == null || fileInformation.getStatus() != Index.FileStatus.STORE_COMPLETE){
            sendMessage(controllerSocket, "ERROR_FILE_DOES_NOT_EXIST");
            System.err.println("ERROR: File " + filename + " does not exist");
            return;
        }
        if (dstorePortsSocketsMap.size() < replicationFactor){
            sendMessage(controllerSocket, "ERROR_NOT_ENOUGH_DSTORES");
            System.err.println("ERROR: Not enough Dstores to remove " + filename);
            return;
        }

        fileInformation.setStatus(Index.FileStatus.REMOVE_IN_PROGRESS);

        ArrayList<Integer> dStoresWithFile = fileInformation.getStoragePorts();
        CountDownLatch latch = new CountDownLatch(dStoresWithFile.size());
        removeCountdownMap.put(filename, latch);

        for (Integer port : dStoresWithFile) {
            new Thread(() -> {
                try {
                    Socket dstoreSocket = dstorePortsSocketsMap.get(port);
                    if(dstoreSocket != null && !dstoreSocket.isClosed()){
                        sendMessage(dstoreSocket, "REMOVE " + filename);
                    } else {
                        System.err.println("ERROR: Persistent connection for Dstore " + port + " is unavailable.");
                    }
                } catch (Exception e) {
                    System.err.println("ERROR: Could not connect to Dstore " + port + ": " + e);
                }
            }).start();
        }

        new Thread(() -> {
            try {
                boolean allAcksRecieved = latch.await(timeout, TimeUnit.MILLISECONDS);
                if (allAcksRecieved) {
                    index.getFileInformation(filename).setStatus(Index.FileStatus.REMOVE_COMPLETE);
                    index.removeFile(filename);
                    sendMessage(controllerSocket, "REMOVE_COMPLETE");
                    System.out.println("File " + filename + " removed successfully from Dstores: " + dStoresWithFile);
                } else {
                    System.err.println("ERROR: Timeout waiting for REMOVE_ACK for file " + filename);
                }
            } catch (Exception e) {
                System.err.println("ERROR: Could not wait for REMOVE_ACK: " + e);
            }
        }).start();
    }

    private void handleREMOVE_ACK(String message){
        System.out.println("REMOVE_ACK message recieved!");
        String[] parts = message.split(" ");
        if (parts.length != 2){
            System.err.println("ERROR: Malformed REMOVE_ACK message");
        }
        String filename = parts[1];
        CountDownLatch latch = removeCountdownMap.get(filename);
        if (latch != null){
            latch.countDown();
            System.out.println("Received REMOVE_ACK for file " + filename);
        }
    }

    private void handleListOperation(Socket controllerSocket, String message){
        System.out.println("LIST message recieved!");
        if (dstorePortsSocketsMap.size() < replicationFactor){
            sendMessage(controllerSocket, "ERROR_NOT_ENOUGH_DSTORES");
            System.err.println("ERROR: Not enough Dstores to list files");
            return;
        }
        ArrayList<String> arrayListOfFiles = index.getFileList();
        if (arrayListOfFiles.isEmpty()){
            sendMessage(controllerSocket, "LIST");
            System.out.println("No files stored");
        }else{
            StringBuilder response = new StringBuilder("LIST");
            for (String filename : arrayListOfFiles) {
                response.append(" ").append(filename);
            }
            sendMessage(controllerSocket, response.toString());
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
        int cport = Integer.parseInt(args[0]);
        int replicationFactor = Integer.parseInt(args[1]);
        int timeout = Integer.parseInt(args[2]);
        int rebalance_period = Integer.parseInt(args[3]);

        Controller controller = new Controller(cport, replicationFactor, timeout, rebalance_period);
        controller.start();
    }
}
