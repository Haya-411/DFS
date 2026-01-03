import java.io.*;
import java.net.*;

public class DFSClient {
    private String currentFilePath;
    private String localCache;
    private String accessMode; // "READ", "WRITE", "RW"
    private boolean isDirty = false; // 変更があったか
    private String serverHost;
    private int serverPort = 8080;

    public static void main(String[] args) {
        DFSClient client = new DFSClient();
        try {
            System.out.println("Testing DFSClient...");
            // Open file with read-write mode(rw)
            client.open("localhost", "sample.txt", "RW");
            // Read current content
            System.out.println("Current cache: " + client.read());
            // Rewrite content
            System.out.println("Writing Data...");
            client.write("Hello, Distributed File System!\nThis is a test message.");
            client.close();

            System.out.println("Test completed");

        } catch (IOException e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
        }
    }

    // open: サーバからデータを取得しキャッシュする
    public void open(String siteName, String path, String mode) throws IOException {
        this.serverHost = siteName;
        this.currentFilePath = path;
        this.accessMode = mode;

        try (Socket socket = new Socket(serverHost, serverPort);
                PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {

            out.println("FETCH " + path + " " + mode);
            String status = in.readLine();
            if ("SUCCESS".equals(status)) {
                StringBuilder sb = new StringBuilder();
                String line;
                while ((line = in.readLine()) != null) {
                    sb.append(line).append("\n");
                }
                this.localCache = sb.toString().trim();
                this.isDirty = false;
                System.out.println("Opened: " + path + " [" + mode + "]");
            } else {
                throw new IOException("Server Error: " + status);
            }
        }
    }

    public String read() {
        if (accessMode.equals("WRITE_ONLY"))
            return "Error: Write only";
        return localCache;
    }

    public void write(String newContent) {
        if (accessMode.equals("READ_ONLY")) {
            System.out.println("Error: Read only mode");
            return;
        }
        this.localCache = newContent;
        this.isDirty = true;
    }

    // close: 変更があればサーバへ送る
    public void close() throws IOException {
        if ((accessMode.contains("WRITE") || accessMode.contains("RW")) && isDirty) {
            try (Socket socket = new Socket(serverHost, serverPort);
                    PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                    BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {

                out.println("STORE " + currentFilePath);
                out.print(localCache + "\n__END__\n");
                out.flush();

                if ("SUCCESS".equals(in.readLine())) {
                    System.out.println("Changes saved to server.");
                }
            }
        }
        this.localCache = null;
        this.currentFilePath = null;
        System.out.println("Client closed.");
    }
}
