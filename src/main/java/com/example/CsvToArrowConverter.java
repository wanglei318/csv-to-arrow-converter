package com.example;

import com.example.utils.BomUtils;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

public class CsvToArrowConverter {
    private static final Logger logger = LoggerFactory.getLogger(CsvToArrowConverter.class);
    private static final int BATCH_SIZE = 1000;

    public static void main(String[] args) {
        if (args.length != 2) {
            logger.error("使用方法: java -jar csv-to-arrow-converter.jar <输入CSV文件> <输出Arrow文件>");
            System.exit(1);
        }

        String inputFile = args[0];
        String outputFile = args[1];
        
        try {
            convertCsvToArrow(inputFile, outputFile);
            logger.info("转换完成！输出文件: {}", outputFile);
        } catch (Exception e) {
            logger.error("转换过程中发生错误", e);
            System.exit(1);
        }
    }

    public static String process(String inputFile) {
        String outputFile = inputFile.replace("csv", "feather");

        try {
            convertCsvToArrow(inputFile, outputFile);
            logger.info("转换完成！输出文件: {}", outputFile);
            return outputFile;
        } catch (Exception e) {
            logger.error("转换过程中发生错误", e);
            return null;
        }
    }

    public static void convertCsvToArrow(String inputCsvPath, String outputArrowPath) throws IOException {
        BufferAllocator allocator = new RootAllocator();
        File inputFile = new File(inputCsvPath);
        
        try {
            // 使用BomUtils创建不带BOM的Reader
            Reader reader = BomUtils.createReaderWithoutBOM(inputFile);
            
            // 读取CSV文件头
            CSVParser parser = CSVFormat.DEFAULT.withFirstRecordAsHeader()
                    .parse(reader);
            
            List<Field> fields = new ArrayList<>();
            for (String header : parser.getHeaderNames()) {
                fields.add(new Field(header, FieldType.nullable(new ArrowType.Utf8()), null));
            }

            // 创建VectorSchemaRoot
            VectorSchemaRoot root = VectorSchemaRoot.create(
                    new Schema(fields), allocator);

            // 创建Arrow文件写入器
            File outputFile = new File(outputArrowPath);
            FileChannel channel = FileChannel.open(
                    outputFile.toPath(),
                    StandardOpenOption.CREATE,
                    StandardOpenOption.WRITE
            );

            org.apache.arrow.vector.ipc.ArrowFileWriter writer = null;
            try {
                writer = new org.apache.arrow.vector.ipc.ArrowFileWriter(
                        root, null, channel);

                writer.start();
                List<VarCharVector> vectors = new ArrayList<>();

                // 为每个列创建向量
                for (Field field : fields) {
                    VarCharVector vector = (VarCharVector) root.getVector(field.getName());
                    vectors.add(vector);
                    vector.allocateNew();
                }

                // 分批处理CSV记录
                int rowCount = 0;
                for (CSVRecord record : parser) {
                    if (rowCount >= BATCH_SIZE) {
                        root.setRowCount(rowCount);
                        writer.writeBatch();
                        rowCount = 0;
                        for (VarCharVector vector : vectors) {
                            vector.reset();
                        }
                    }

                    for (int i = 0; i < record.size(); i++) {
                        String value = record.get(i);
                        vectors.get(i).setSafe(rowCount, value.getBytes());
                    }
                    rowCount++;
                }

                // 写入最后一批数据
                if (rowCount > 0) {
                    root.setRowCount(rowCount);
                    writer.writeBatch();
                }
            } finally {
                if (writer != null) {
                    writer.close();
                }
                channel.close();
                root.close();
                reader.close();
            }
        } finally {
            allocator.close();
        }
    }
}