import java.net.*;

public class Controller {
    private int cport;
    private int replicationFactor;
    private int timeout;
    private int rebalance_period;
    static Index index;

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
                    Socket controllerClient = controllerServerSocket.accept();
                    System.out.println("Connected Successfully on port " + controllerClient.getPort() + " with address " + controllerClient.getInetAddress());
                }catch(Exception e){
                    System.err.println("ERROR: Could not accept connection: " + e);
                }
            }
        }catch(Exception e){
            System.err.println("ERROR: Could not create controller socket: " + e);
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
