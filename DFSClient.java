import java.io.*;
import java.net.*;
import java.util.*;

public class DFSClient {
    private static String currentFile;
    private static String mode;
    private static int version; // バージョン番号
    private static boolean dirty = false; // 変更があったか

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
                default:
                    System.out.println("Unknown command");
            }
        }
    }

    private static void handleOpen(String[] cmd, PrintWriter out, BufferedReader in) throws IOException {
        out.println("OPEN " + cmd[1] + " " + cmd[2]);
        String status = in.readLine();
        if (status.startsWith("ERROR")) {
            System.out.println(status);
            return;
        }

        int serverVersion = Integer.parseInt(status.split(" ")[1]);
        String serverContent = in.readLine();

        // キャッシュの更新または新規作成
        lruCache.put(cmd[1], new CacheEntry(serverContent, serverVersion));
        currentFile = cmd[1];
        mode = cmd[2];
        System.out.println("Opened " + currentFile + " (v" + serverVersion + ")");
    }

    private static void handleWrite(String[] cmd) {
        if (currentFile == null || !mode.equals("WRITE")) {
            System.out.println("ERROR: Not opened for WRITE");
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
            out.println("UPDATE " + currentFile + " " + entry.version);
            out.println(entry.content);
            String res = in.readLine();
            if (res.startsWith("OK")) {
                entry.version = Integer.parseInt(res.split(" ")[1]);
                entry.dirty = false;
                System.out.println("Server updated to v" + entry.version);
            } else {
                System.out.println("Update failed: " + res);
            }
        }
        out.println("CLOSE " + currentFile);
        System.out.println("Closed and lock released.");
        currentFile = null;
    }
}
