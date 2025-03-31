package com.example.utils;

import java.io.*;

public class BomUtils {
    private static final byte[] UTF8_BOM = new byte[]{(byte) 0xEF, (byte) 0xBB, (byte) 0xBF};

    /**
     * 检查输入流是否包含BOM头
     * @param is 输入流
     * @return 如果包含BOM头返回true
     * @throws IOException IO异常
     */
    public static boolean hasBOM(InputStream is) throws IOException {
        is.mark(3);
        byte[] bom = new byte[3];
        int bytesRead = is.read(bom);
        is.reset();
        
        if (bytesRead != 3) {
            return false;
        }
        
        return bom[0] == UTF8_BOM[0] && 
               bom[1] == UTF8_BOM[1] && 
               bom[2] == UTF8_BOM[2];
    }

    /**
     * 创建一个跳过BOM头的Reader
     * @param file 输入文件
     * @return 处理后的Reader
     * @throws IOException IO异常
     */
    public static Reader createReaderWithoutBOM(File file) throws IOException {
        FileInputStream fis = new FileInputStream(file);
        BufferedInputStream bis = new BufferedInputStream(fis);
        
        // 检查并跳过BOM
        if (hasBOM(bis)) {
            bis.skip(3);
        }
        
        return new InputStreamReader(bis);
    }
}