package org.apache.iceberg;

import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

public class MetadataField extends Types.NestedField {

  public MetadataField(boolean isOptional, int id, String name, Type type, String doc) {
    super(isOptional, id, name, type, doc);
  }
}
