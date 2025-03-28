package com.example;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class CsvToArrowConverter {
    private static final Logger logger = LoggerFactory.getLogger(CsvToArrowConverter.class);
    private static final int BATCH_SIZE = 1000;

    public static void main(String[] args) {
        if (args.length != 2) {
            System.out.println("Usage: java -jar csv-to-arrow-converter.jar <input.csv> <output.arrow>");
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

    public static void convertCsvToArrow(String inputCsvPath, String outputArrowPath) throws IOException {
        try (BufferAllocator allocator = new RootAllocator()) {
            // 读取CSV文件头
            CSVParser parser = CSVFormat.DEFAULT.withFirstRecordAsHeader()
                    .parse(new FileReader(inputCsvPath));
            
            List<Field> fields = new ArrayList<>();
            for (String header : parser.getHeaderNames()) {
                fields.add(new Field(header, FieldType.nullable(new ArrowType.Utf8()), null));
            }

            // 创建VectorSchemaRoot
            VectorSchemaRoot root = VectorSchemaRoot.create(
                    org.apache.arrow.vector.types.pojo.Schema.new_(fields), allocator);

            // 创建Arrow文件写入器
            File outputFile = new File(outputArrowPath);
            try (org.apache.arrow.vector.ipc.ArrowFileWriter writer = 
                    new org.apache.arrow.vector.ipc.ArrowFileWriter(root, null, 
                            java.nio.channels.FileChannel.open(outputFile.toPath(), 
                                    java.nio.file.StandardOpenOption.CREATE, 
                                    java.nio.file.StandardOpenOption.WRITE))) {

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
            }
        }
    }
}