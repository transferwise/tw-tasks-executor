package com.transferwise.tasks.dao;

import com.transferwise.common.baseutils.ExceptionUtils;
import com.transferwise.tasks.CompressionAlgorithm;
import com.transferwise.tasks.TasksProperties;
import java.io.InputStream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import net.jpountz.lz4.LZ4BlockInputStream;
import net.jpountz.lz4.LZ4BlockOutputStream;
import net.jpountz.lz4.LZ4Factory;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.input.UnsynchronizedByteArrayInputStream;
import org.apache.commons.io.output.UnsynchronizedByteArrayOutputStream;
import org.springframework.beans.factory.annotation.Autowired;

public class TaskDataSerializer implements ITaskDataSerializer {

  private static final int COMPRESSION_NONE = 0;
  private static final int COMPRESSION_GZIP = 1;
  private static final int COMPRESSION_LZ4 = 2;

  @Autowired
  private TasksProperties tasksProperties;

  @Override
  public SerializeResult serialize(byte[] inputData) {
    return ExceptionUtils.doUnchecked(() -> {
      final SerializeResult result = new SerializeResult();

      CompressionAlgorithm algorithm = tasksProperties.getCompression().getAlgorithm();
      if (algorithm == CompressionAlgorithm.RANDOM) {
        algorithm = CompressionAlgorithm.getRandom();
      }

      if (algorithm == CompressionAlgorithm.GZIP) {
        UnsynchronizedByteArrayOutputStream bos = new UnsynchronizedByteArrayOutputStream();
        try (GZIPOutputStream compressOut = new GZIPOutputStream(bos)) {
          compressOut.write(inputData);
        }
        result.setDataFormat(COMPRESSION_GZIP);
        result.setData(bos.toByteArray());
      } else if (algorithm == CompressionAlgorithm.LZ4) {
        UnsynchronizedByteArrayOutputStream bos = new UnsynchronizedByteArrayOutputStream();
        try (LZ4BlockOutputStream compressOut = new LZ4BlockOutputStream(bos, 32 * 1024,
            LZ4Factory.fastestJavaInstance().fastCompressor())) {
          compressOut.write(inputData);
        }
        result.setDataFormat(COMPRESSION_LZ4);
        result.setData(bos.toByteArray());
      } else {
        result.setDataFormat(COMPRESSION_NONE);
        result.setData(inputData);
      }

      return result;
    });
  }

  @Override
  public byte[] deserialize(int dataFormat, byte[] data) {
    return ExceptionUtils.doUnchecked(() -> {
      if (data == null) {
        return null;
      }

      if (dataFormat == COMPRESSION_GZIP) {
        UnsynchronizedByteArrayInputStream bis = new UnsynchronizedByteArrayInputStream(data);
        try (InputStream is = new GZIPInputStream(bis)) {
          return IOUtils.toByteArray(is);
        }
      }

      if (dataFormat == COMPRESSION_LZ4) {
        UnsynchronizedByteArrayInputStream bis = new UnsynchronizedByteArrayInputStream(data);
        try (InputStream is = new LZ4BlockInputStream(bis, LZ4Factory.fastestJavaInstance().fastDecompressor())) {
          return IOUtils.toByteArray(is);
        }
      }

      return data;
    });
  }
}
