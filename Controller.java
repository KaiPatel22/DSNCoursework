public class Controller {
    private int cport;
    private int replicationFactor;
    private int timeout;
    private int rebalance_period;

    public Controller(int cport, int replicationFactor, int timeout, int rebalance_period) {
        this.cport = cport;
        this.replicationFactor = replicationFactor;
        this.timeout = timeout;
        this.rebalance_period = rebalance_period;
    }

    public static void main(String[] args) {
        try {
            int cport = Integer.parseInt(args[0]);
            int replicationFactor = Integer.parseInt(args[1]);
            int timeout = Integer.parseInt(args[2]);
            int rebalance_period = Integer.parseInt(args[3]);
        } catch (Exception e) {
            System.err.println("Error: Parameters are not being passed correctly");
        }

        Controller controller = new Controller(cport, replicationFactor, timeout, rebalance_period);
    }
}
