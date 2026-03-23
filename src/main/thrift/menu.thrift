namespace java com.serialization.lab.thrift

struct MenuItem {
  1: required string id,
  2: optional string label
}

struct ItemOrNull {
  1: required bool hasValue,
  2: optional MenuItem item
}

struct Menu {
  1: required string header,
  2: required list<ItemOrNull> items
}