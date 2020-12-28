package com.transferwise.tasks.dao;

import com.github.luben.zstd.ZstdInputStream;
import com.github.luben.zstd.ZstdOutputStream;
import com.transferwise.common.baseutils.ExceptionUtils;
import com.transferwise.tasks.CompressionAlgorithm;
import com.transferwise.tasks.ITasksService.AddTaskRequest.CompressionRequest;
import com.transferwise.tasks.TasksProperties;
import java.io.IOException;
import java.io.InputStream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import javax.annotation.Nonnull;
import net.jpountz.lz4.LZ4BlockInputStream;
import net.jpountz.lz4.LZ4BlockOutputStream;
import net.jpountz.lz4.LZ4Factory;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.input.UnsynchronizedByteArrayInputStream;
import org.apache.commons.io.output.UnsynchronizedByteArrayOutputStream;
import org.springframework.beans.factory.annotation.Autowired;

public class TaskDaoDataSerializer implements ITaskDaoDataSerializer {

  private static final int COMPRESSION_NONE = 0;
  private static final int COMPRESSION_GZIP = 1;
  private static final int COMPRESSION_LZ4 = 2;
  private static final int COMPRESSION_ZSTD = 3;

  @Autowired
  private TasksProperties tasksProperties;

  @Override
  public SerializedData serialize(@Nonnull byte[] inputData, CompressionRequest compressionRequest) {
    return ExceptionUtils.doUnchecked(() -> {
      SerializedData serializedData = compress(inputData, compressionRequest);
      if (serializedData == null || serializedData.getData().length > inputData.length) {
        return doNotCompress(inputData);
      }
      return serializedData;
    });
  }

  protected SerializedData compress(byte[] inputData, CompressionRequest compression) throws IOException {
    if (compression == null) {
      TasksProperties.Compression globalCompression = tasksProperties.getCompression();
      if (inputData.length >= globalCompression.getMinSize()) {
        compression = new CompressionRequest().setAlgorithm(globalCompression.getAlgorithm())
            .setBlockSizeBytes(globalCompression.getBlockSizeBytes()).setLevel(globalCompression.getLevel());
      }
    }

    if (compression == null || compression.getAlgorithm() == null || compression.getAlgorithm() == CompressionAlgorithm.NONE) {
      return null;
    }

    CompressionAlgorithm algorithm = compression.getAlgorithm();
    if (algorithm == CompressionAlgorithm.RANDOM) {
      algorithm = CompressionAlgorithm.getRandom();
    }

    if (algorithm == CompressionAlgorithm.NONE) {
      return null;
    }
    if (algorithm == CompressionAlgorithm.GZIP) {
      return compressGzip(inputData);
    }
    if (algorithm == CompressionAlgorithm.LZ4) {
      return compressLz4(inputData, compression);
    }
    if (algorithm == CompressionAlgorithm.ZSTD) {
      return compressZstd(inputData, compression);
    }

    throw new IllegalStateException("No support for compression algorithm of " + algorithm + ".");
  }

  protected SerializedData doNotCompress(byte[] inputData) {
    return new SerializedData().setDataFormat(COMPRESSION_NONE).setData(inputData);
  }

  protected SerializedData compressLz4(byte[] inputData, CompressionRequest compression) throws IOException {
    UnsynchronizedByteArrayOutputStream bos = new UnsynchronizedByteArrayOutputStream();
    try (LZ4BlockOutputStream compressOut = new LZ4BlockOutputStream(bos,
        compression.getBlockSizeBytes() == null ? 1 << 16 : compression.getBlockSizeBytes(),
        LZ4Factory.fastestJavaInstance().fastCompressor())) {
      compressOut.write(inputData);
    }
    return new SerializedData().setDataFormat(COMPRESSION_LZ4).setData(bos.toByteArray());
  }

  protected SerializedData compressGzip(byte[] inputData) throws IOException {
    UnsynchronizedByteArrayOutputStream bos = new UnsynchronizedByteArrayOutputStream();
    try (GZIPOutputStream compressOut = new GZIPOutputStream(bos)) {
      compressOut.write(inputData);
    }
    return new SerializedData().setDataFormat(COMPRESSION_GZIP).setData(bos.toByteArray());
  }

  protected SerializedData compressZstd(byte[] inputData, CompressionRequest compressionRequest) throws IOException {
    UnsynchronizedByteArrayOutputStream bos = new UnsynchronizedByteArrayOutputStream();
    if (compressionRequest.getLevel() != null) {
      try (ZstdOutputStream compressOut = new ZstdOutputStream(bos, compressionRequest.getLevel())) {
        compressOut.write(inputData);
      }
    } else {
      try (ZstdOutputStream compressOut = new ZstdOutputStream(bos)) {
        compressOut.write(inputData);
      }
    }
    return new SerializedData().setDataFormat(COMPRESSION_ZSTD).setData(bos.toByteArray());
  }

  @Override
  public byte[] deserialize(SerializedData serializedData) {
    return ExceptionUtils.doUnchecked(() -> {
      if (serializedData == null) {
        return null;
      }

      final byte[] data = serializedData.getData();
      if (data == null) {
        return null;
      }
      final int dataFormat = serializedData.getDataFormat();

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

      if (dataFormat == COMPRESSION_ZSTD) {
        UnsynchronizedByteArrayInputStream bis = new UnsynchronizedByteArrayInputStream(data);
        try (InputStream is = new ZstdInputStream(bis)) {
          return IOUtils.toByteArray(is);
        }
      }

      return serializedData.getData();
    });
  }
}
