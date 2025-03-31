# CSV to Arrow 转换器

这是一个简单而高效的命令行工具，用于将 CSV 文件转换为 Apache Arrow 格式。

## 功能特点

- 支持任意列的 CSV 文件转换
- 自动识别 CSV 文件头
- 自动检测和处理UTF-8 BOM头
- 批量处理以优化内存使用
- 支持大文件处理
- 生成标准的 Arrow 文件格式输出

## 系统要求

- Java 1.8 或更高版本
- Maven 3.6 或更高版本

## 构建说明

1. 克隆仓库：
```bash
git clone https://github.com/wanglei318/csv-to-arrow-converter.git
cd csv-to-arrow-converter
```

2. 使用 Maven 构建项目：
```bash
mvn clean package
```

构建完成后，可执行的 JAR 文件将位于 `target` 目录中。

## 使用方法

基本用法：
```bash
java -jar target/csv-to-arrow-converter-1.0-SNAPSHOT-jar-with-dependencies.jar <输入CSV文件> <输出Arrow文件>
```

示例：
```bash
java -jar target/csv-to-arrow-converter-1.0-SNAPSHOT-jar-with-dependencies.jar data.csv output.arrow
```

## 特性说明

### BOM头处理
本工具会自动检测并处理CSV文件中的UTF-8 BOM头，确保数据正确读取。无需手动处理BOM头问题，工具会自动：
- 检测文件是否包含BOM头
- 如果存在BOM头，自动跳过
- 确保输出的Arrow文件中不包含BOM头

## 示例

假设有一个名为 `data.csv` 的输入文件，内容如下：
```csv
姓名,年龄,城市
张三,25,北京
李四,30,上海
王五,28,广州
```

运行转换命令：
```bash
java -jar target/csv-to-arrow-converter-1.0-SNAPSHOT-jar-with-dependencies.jar data.csv output.arrow
```

这将生成一个名为 `output.arrow` 的 Apache Arrow 格式文件。

## 技术栈

- Java 1.8
- Apache Arrow
- Apache Commons CSV
- SLF4J + Logback
- JUnit 5

## 许可证

本项目采用 MIT 许可证。详情请参见 [LICENSE](LICENSE) 文件。