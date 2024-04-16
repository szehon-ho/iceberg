/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.parquet;

import static org.apache.iceberg.Files.localInput;
import static org.apache.iceberg.Files.localOutput;
import static org.apache.iceberg.parquet.ParquetWritingTestUtils.createTempFile;
import static org.apache.iceberg.types.Types.NestedField.optional;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.List;
import java.util.Random;
import org.apache.avro.generic.GenericData;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TestHelpers;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.encryption.NativeFileCryptoParameters;
import org.apache.iceberg.encryption.NativelyEncryptedFile;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types.IntegerType;
import org.apache.parquet.crypto.ParquetCryptoRuntimeException;
import org.junit.Assert;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestParquetEncryption {

  private static final String columnName = "intCol";
  private static final int recordCount = 100;
  private static final byte[] fileDek = new byte[16];
  private static File file;
  private static final Schema schema = new Schema(optional(1, columnName, IntegerType.get()));

  private static class EncryptedLocalOutputFile implements OutputFile, NativelyEncryptedFile {
    private OutputFile localOutputFile;
    private NativeFileCryptoParameters nativeEncryptionParameters;

    private EncryptedLocalOutputFile(File file) {
      localOutputFile = localOutput(file);
    }

    @Override
    public PositionOutputStream create() {
      return localOutputFile.create();
    }

    @Override
    public PositionOutputStream createOrOverwrite() {
      return localOutputFile.createOrOverwrite();
    }

    @Override
    public String location() {
      return localOutputFile.location();
    }

    @Override
    public InputFile toInputFile() {
      return localOutputFile.toInputFile();
    }

    @Override
    public NativeFileCryptoParameters nativeCryptoParameters() {
      return nativeEncryptionParameters;
    }

    @Override
    public void setNativeCryptoParameters(NativeFileCryptoParameters nativeCryptoParameters) {
      this.nativeEncryptionParameters = nativeCryptoParameters;
    }
  }

  private static class EncryptedLocalInputFile implements InputFile, NativelyEncryptedFile {
    private InputFile localInputFile;
    private NativeFileCryptoParameters nativeDecryptionParameters;

    private EncryptedLocalInputFile(File file) {
      localInputFile = localInput(file);
    }

    @Override
    public long getLength() {
      return localInputFile.getLength();
    }

    @Override
    public SeekableInputStream newStream() {
      return localInputFile.newStream();
    }

    @Override
    public String location() {
      return localInputFile.location();
    }

    @Override
    public boolean exists() {
      return localInputFile.exists();
    }

    @Override
    public NativeFileCryptoParameters nativeCryptoParameters() {
      return nativeDecryptionParameters;
    }

    @Override
    public void setNativeCryptoParameters(NativeFileCryptoParameters nativeCryptoParameters) {
      this.nativeDecryptionParameters = nativeCryptoParameters;
    }
  }

  @TempDir private Path temp;

  @BeforeEach
  private void writeEncryptedFile() throws IOException {
    List<GenericData.Record> records = Lists.newArrayListWithCapacity(recordCount);
    org.apache.avro.Schema avroSchema = AvroSchemaUtil.convert(schema.asStruct());
    for (int i = 1; i <= recordCount; i++) {
      GenericData.Record record = new GenericData.Record(avroSchema);
      record.put(columnName, i);
      records.add(record);
    }

    Random rand = new Random();
    rand.nextBytes(fileDek);

    NativeFileCryptoParameters encryptionParams =
        NativeFileCryptoParameters.create(ByteBuffer.wrap(fileDek)).build();

    file = createTempFile(temp);

    EncryptedLocalOutputFile outputFile = new EncryptedLocalOutputFile(file);
    outputFile.setNativeCryptoParameters(encryptionParams);

    FileAppender<GenericData.Record> writer = Parquet.write(outputFile).schema(schema).build();

    try (Closeable toClose = writer) {
      writer.addAll(Lists.newArrayList(records.toArray(new GenericData.Record[] {})));
    }
  }

  @Test
  public void testReadEncryptedFileWithoutKeys() throws IOException {
    writeEncryptedFile();

    TestHelpers.assertThrows(
        "Decrypted without keys",
        ParquetCryptoRuntimeException.class,
        "Trying to read file with encrypted footer. No keys available",
        () -> Parquet.read(localInput(file)).project(schema).callInit().build().iterator().next());
  }

  @Test
  public void testReadEncryptedFile() throws IOException {
    writeEncryptedFile();

    NativeFileCryptoParameters encryptionParams =
        NativeFileCryptoParameters.create(ByteBuffer.wrap(fileDek)).build();

    EncryptedLocalInputFile inputFile = new EncryptedLocalInputFile(file);
    inputFile.setNativeCryptoParameters(encryptionParams);

    try (CloseableIterator readRecords =
        Parquet.read(inputFile).project(schema).callInit().build().iterator()) {
      for (int i = 1; i <= recordCount; i++) {
        GenericData.Record readRecord = (GenericData.Record) readRecords.next();
        Assert.assertEquals(i, readRecord.get(columnName));
      }
    }
  }
}
