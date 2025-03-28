# CSV to Apache Arrow 转换器

这是一个简单的Java命令行工具，用于将CSV文件转换为Apache Arrow格式。

## 功能特点

- 支持任意列数的CSV文件
- 自动识别CSV文件头
- 批量处理数据，内存占用可控
- 使用Apache Arrow的高效列式存储格式

## 系统要求

- Java 17 或更高版本
- Maven 3.6 或更高版本

## 构建说明

1. 克隆仓库：
   ```bash
   git clone https://github.com/wanglei318/csv-to-arrow-converter.git
   cd csv-to-arrow-converter
   ```

2. 使用Maven构建项目：
   ```bash
   mvn clean package
   ```

## 使用方法

运行已打包的JAR文件：

```bash
java -jar target/csv-to-arrow-converter-1.0-SNAPSHOT-jar-with-dependencies.jar input.csv output.arrow
```

参数说明：
- `input.csv`: 输入的CSV文件路径
- `output.arrow`: 输出的Arrow文件路径

## 注意事项

- CSV文件必须包含表头
- 目前所有数据都被视为字符串类型处理
- 建议对大文件进行转换时适当调整JVM内存参数

## 示例

假设有一个名为 `data.csv` 的文件：
```csv
name,age,city
张三,25,北京
李四,30,上海
```

转换命令：
```bash
java -jar target/csv-to-arrow-converter-1.0-SNAPSHOT-jar-with-dependencies.jar data.csv data.arrow
```

## 许可证

MIT License