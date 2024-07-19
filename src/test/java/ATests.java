/**
 * @author zongkx
 */

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.reflect.ReflectData;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.io.LocalOutputFile;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;

import static org.apache.parquet.hadoop.ParquetFileWriter.Mode.OVERWRITE;
import static org.apache.parquet.hadoop.metadata.CompressionCodecName.SNAPPY;


/**
 * @author zongkx
 */

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Slf4j
class ATests {

    @BeforeAll
    public void before() {


    }


    @Test
    @SneakyThrows
    void a1() {
        write(Collections.singletonList(new User(1L, "测试")));
    }

    @SneakyThrows
    private <T> void write(List<T> data) {
        Path dataFile = Paths.get("D:\\demo.parquet");
        LocalOutputFile localOutputFile = new LocalOutputFile(dataFile);
        try (ParquetWriter<T> writer = AvroParquetWriter.<T>builder(localOutputFile)
                .withSchema(ReflectData.AllowNull.get().getSchema(data.get(0).getClass()))
                .withDataModel(ReflectData.get())
                .withConf(new Configuration())
                .withCompressionCodec(SNAPPY)
                .withWriteMode(OVERWRITE)
                .withWriterVersion(ParquetProperties.WriterVersion.PARQUET_2_0)
                .withValidation(false)
                .enableValidation()
                .build()) {
            for (T t : data) {
                writer.write(t);
            }
        }
    }

    @AllArgsConstructor
    @NoArgsConstructor
    @Data
    static class User {
        private Long id;
        private String name;
    }
}