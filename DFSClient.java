import java.io.*;
import java.net.*;
import java.util.*;
import java.util.regex.*;

public class DFSClient {
    private static String currentFile;
    private static String mode;
    private static int version; // バージョン番号
    private static boolean dirty = false; // 変更があったか
    private static Socket serverSocket = null;//子サーバーとの接続ソケット
    private static BufferedReader inServer;//子サーバーからの入力ストリーム
    private static PrintWriter outServer;//子サーバーへの出力ストリーム


    // キャッシュ情報の構造体
    static class CacheEntry {
        String content;
        int version;
        boolean dirty;

        CacheEntry(String content, int version) {
            this.content = content;
            this.version = version;
            this.dirty = false;
        }
    }

    // LRUキャッシュ
    private static final int MAX_CACHE_SIZE = 3; // 最大キャッシュ数を設定
    private static Map<String, CacheEntry> lruCache = new LinkedHashMap<>(MAX_CACHE_SIZE, 0.75f, true) {
        protected boolean removeEldestEntry(Map.Entry<String, CacheEntry> eldest) {
            if (size() > MAX_CACHE_SIZE) {
                System.out.println("[Cache] Evicting oldest file: " + eldest.getKey());
                return true;
            }
            return false;
        }
    };

    public static void main(String[] args) throws Exception {
        Socket socket = new Socket("localhost", 8080);
        BufferedReader in = new BufferedReader(
                new InputStreamReader(socket.getInputStream()));
        PrintWriter out = new PrintWriter(
                socket.getOutputStream(), true);

        
        Scanner scanner = new Scanner(System.in);
        System.out.println("DFS Client started. (Max Cache: " + MAX_CACHE_SIZE + ")");

        while (true) {
            System.out.print("> ");
            String line = scanner.nextLine();
            String[] cmd = line.split(" ");
            if (cmd.length == 0)
                continue;

            switch (cmd[0].toUpperCase()) {
                case "OPEN":
                    handleOpen(cmd, out, in);
                    break;
                case "READ":
                    if (currentFile != null)
                        System.out.println(lruCache.get(currentFile).content);
                    break;
                case "WRITE":
                    handleWrite(cmd);
                    break;
                case "CLOSE":
                    handleClose(out, in);
                    break;
                case "LIST":
                    handleList(out, in);
                    break;
                
                default:
                    System.out.println("Unknown command");
            }
        }
    }

    private static void handleOpen(String[] cmd, PrintWriter outMaster, BufferedReader inMaster) throws IOException {
        if (cmd.length < 3) {
        System.out.println("ERROR: OPEN requires at least 2 arguments (file path and mode)");
        return;
        }

        Pattern patternRead = Pattern.compile("R|READ(?:[_-]ONLY)?");//R,READ,READ_ONLY,READ-ONLYを受け付ける
        Matcher matcherRead = patternRead.matcher(cmd[2].toUpperCase());

        Pattern patternWrite = Pattern.compile("W|WRITE(?:[_-]ONLY)?");//W,WRITE,WRITE_ONLY,WRITE-ONLYを受け付ける
        Matcher matcherWrite = patternWrite.matcher(cmd[2].toUpperCase());

        if(matcherRead.matches()){
            mode = "READ";
        }else if(matcherWrite.matches()){
            mode = "WRITE";
        }else{//それ以外はRead-Writeにする
            mode = "RW";
        }

        outMaster.println("OPEN " + cmd[1] + " " + mode);
        String host = cmd[1].substring(0, cmd[1].indexOf('/', 2));
        String fileName = cmd[1].substring(cmd[1].indexOf('/', 2) + 1);
        String statusMaster = inMaster.readLine();

        if (statusMaster.startsWith("[ERROR]")) {
            System.out.println(statusMaster);
            return;
        }

        int serverPort = Integer.parseInt(statusMaster.split(" ")[1]);
        serverSocket = new Socket("localhost", serverPort);
        inServer = new BufferedReader(new InputStreamReader(serverSocket.getInputStream()));
        outServer = new PrintWriter(serverSocket.getOutputStream(), true);

        if(inServer == null || outServer == null){
            System.out.println("Failed to connect to DFS Server");
            return;
        }

        outServer.println("OPEN " + fileName + " " + mode);//子サーバに対してOPENコマンドを送信
        String statusServer = inServer.readLine();
        
        if (statusServer.startsWith("[ERROR]")) {
            System.out.println(statusServer);
            return;
        }
        int serverVersion = Integer.parseInt(statusServer.split(" ")[1]);
        String serverContent = inServer.readLine();

        // キャッシュの更新または新規作成
        lruCache.put(fileName, new CacheEntry(serverContent, serverVersion));
        currentFile = fileName;
        System.out.println("Opened " + currentFile + " (v" + serverVersion + ")");
    }

    private static void handleWrite(String[] cmd) {
        if (currentFile == null || mode.equals("READ")) {
            System.out.println("[ERROR]: Not opened for WRITE");
            return;
        }
        CacheEntry entry = lruCache.get(currentFile);
        entry.content += cmd[1];
        entry.dirty = true;
        System.out.println("Updated local cache (dirty)");
    }

    private static void handleClose(PrintWriter out, BufferedReader in) throws IOException {
        if (currentFile == null)
            return;

        CacheEntry entry = lruCache.get(currentFile);
        if (entry.dirty) {
           
            outServer.println("UPDATE " + currentFile + " " + entry.version);
            outServer.println(entry.content);
            
            String res = inServer.readLine();
            if (res.startsWith("OK")) {
                entry.version = Integer.parseInt(res.split(" ")[1]);
                entry.dirty = false;
                System.out.println("Server updated to v" + entry.version);
            } else {
                System.out.println("Update failed: " + res);
            }
        }
        outServer.println("CLOSE " + currentFile);
        if(inServer.readLine().equals("OK")) System.out.println("Closed and lock released.");
        
        out.println("CLOSE");
        if(in.readLine().equals("OK")) System.out.println("Closing a file Succeeded.");
        
        
        currentFile = null;
        inServer = null;//子サーバーとの接続を閉じる
        outServer = null;
        serverSocket.close();     

    }

    private static void handleList(PrintWriter out, BufferedReader in) throws IOException {
        out.println("LIST");
        String line;
            
        while (!(line = in.readLine()).equals("FILES_LIST_END")) { 
            if (!line.equals("FILES_LIST_START")) {
                System.out.println(line);
            }
        }
        return;
    }
}
