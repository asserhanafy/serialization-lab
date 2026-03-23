package com.serialization.lab;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TCompactProtocol;
import org.msgpack.jackson.dataformat.MessagePackMapper;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.serialization.lab.proto.MenuProto;
import com.serialization.lab.thrift.ItemOrNull;
import com.serialization.lab.thrift.Menu;
import com.serialization.lab.thrift.MenuItem;

public class App {

    static final String JSON = "{\"menu\":{\"header\":\"SVG Viewer\",\"items\":[{\"id\":\"Open\"},{\"id\":\"OpenNew\",\"label\":\"Open New\"},null,{\"id\":\"ZoomIn\",\"label\":\"Zoom In\"},{\"id\":\"ZoomOut\",\"label\":\"Zoom Out\"},{\"id\":\"OriginalView\",\"label\":\"Original View\"},null,{\"id\":\"Quality\"},{\"id\":\"Pause\"},{\"id\":\"Mute\"},null,{\"id\":\"Find\",\"label\":\"Find...\"},{\"id\":\"FindAgain\",\"label\":\"Find Again\"},{\"id\":\"Copy\"},{\"id\":\"CopyAgain\",\"label\":\"Copy Again\"},{\"id\":\"CopySVG\",\"label\":\"Copy SVG\"},{\"id\":\"ViewSVG\",\"label\":\"View SVG\"},{\"id\":\"ViewSource\",\"label\":\"View Source\"},{\"id\":\"SaveAs\",\"label\":\"Save As\"},null,{\"id\":\"Help\"},{\"id\":\"About\",\"label\":\"About Adobe CVG Viewer...\"}]}}";

    public static void main(String[] args) throws Exception {

        byte[] jsonBytes = JSON.getBytes("UTF-8");
        System.out.println("=== File Sizes (1 message) ===");
        System.out.println("Original JSON : " + jsonBytes.length + " bytes");

        serializeMessagePack(1);

        serializeAvro(1);
        serializeProtobuf();
        serializeThrift();

        System.out.println("\n=== File Sizes (100 messages) ===");
        byte[] json100 = buildJson100();
        System.out.println("Original JSON x100 : " + json100.length + " bytes");

        serializeMessagePack100();

        serializeAvro100();
        serializeProtobuf100();
        serializeThrift100();
    }

    // ---------------------------------------------------------------
    // MessagePack - 1 message
    // ---------------------------------------------------------------
    static void serializeMessagePack(int count) throws Exception {
        // Step 1: Parse the JSON string into a JsonNode tree
        ObjectMapper jsonMapper = new ObjectMapper();
        JsonNode tree = jsonMapper.readTree(JSON);

        // Step 2: Create a MessagePackMapper (it works just like ObjectMapper)
        MessagePackMapper msgpackMapper = new MessagePackMapper();

        // Step 3: Serialize the tree to bytes
        byte[] packed = msgpackMapper.writeValueAsBytes(tree);

        // Step 4: Write to file
        Files.write(Paths.get("output_msgpack_1.msgpack"), packed);

        System.out.println("MessagePack (1 msg): " + packed.length + " bytes");
    }

    // ---------------------------------------------------------------
    // MessagePack - 100 messages
    // ---------------------------------------------------------------
    static void serializeMessagePack100() throws Exception {
        ObjectMapper jsonMapper = new ObjectMapper();
        MessagePackMapper msgpackMapper = new MessagePackMapper();

        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        for (int i = 0; i < 100; i++) {
            JsonNode tree = jsonMapper.readTree(JSON);
            byte[] packed = msgpackMapper.writeValueAsBytes(tree);
            baos.write(packed);
        }

        byte[] all = baos.toByteArray();
        Files.write(Paths.get("output_msgpack_100.msgpack"), all);
        System.out.println("MessagePack (100 msg): " + all.length + " bytes");
    }

    // ---------------------------------------------------------------
    // Helper: build JSON repeated 100 times as a JSON array
    // ---------------------------------------------------------------
    static byte[] buildJson100() throws Exception {
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < 100; i++) {
            if (i > 0) sb.append(",");
            sb.append(JSON);
        }
        sb.append("]");
        return sb.toString().getBytes("UTF-8");
    }

    // ---------------------------------------------------------------
    // Avro - 1 message
    // ---------------------------------------------------------------
    static void serializeAvro(int count) throws Exception {
        // Step 1: Load the schema file
        Schema schema = new Schema.Parser().parse(
            new File("src/main/avro/menu.avsc")
        );
        Schema menuItemSchema = schema.getField("items")
            .schema()                  // array schema
            .getElementType()          // union: [null, MenuItem]
            .getTypes().get(1);        // index 1 = the MenuItem record

        // Step 2: Build the list of items (including nulls)
        List<Object> items = buildItems(menuItemSchema);

        // Step 3: Build the top-level Menu record
        GenericRecord menu = new GenericData.Record(schema);
        menu.put("header", "SVG Viewer");
        menu.put("items", items);

        // Step 4: Serialize to bytes using GenericDatumWriter
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DatumWriter<GenericRecord> writer = 
            new GenericDatumWriter<>(schema);
        BinaryEncoder encoder = 
            EncoderFactory.get().binaryEncoder(baos, null);
        writer.write(menu, encoder);
        encoder.flush();

        byte[] avroBytes = baos.toByteArray();
        Files.write(Paths.get("output_avro_1.avro"), avroBytes);
        System.out.println("Avro (1 msg): " + avroBytes.length + " bytes");
    }

    // ---------------------------------------------------------------
    // Avro - 100 message
    // ---------------------------------------------------------------
    static void serializeAvro100() throws Exception {
        Schema schema = new Schema.Parser().parse(
            new File("src/main/avro/menu.avsc")
        );
        Schema menuItemSchema = schema.getField("items")
            .schema()
            .getElementType()
            .getTypes().get(1);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(baos, null);

        for (int i = 0; i < 100; i++) {
            List<Object> items = buildItems(menuItemSchema);

            GenericRecord menu = new GenericData.Record(schema);
            menu.put("header", "SVG Viewer");
            menu.put("items", items);

            writer.write(menu, encoder);
        }

        encoder.flush();
        byte[] all = baos.toByteArray();
        Files.write(Paths.get("output_avro_100.avro"), all);
        System.out.println("Avro (100 msg): " + all.length + " bytes");
    }

    // ---------------------------------------------------------------
    // Helper: Build the list of items (including nulls)
    // ---------------------------------------------------------------
    static List<Object> buildItems(Schema menuItemSchema) {
        List<Object> items = new ArrayList<>();
        GenericRecord r = new GenericData.Record(menuItemSchema);
        r.put("id", "Open");
        r.put("label", null);
        items.add(r);

        r = new GenericData.Record(menuItemSchema);
        r.put("id", "OpenNew");
        r.put("label", "Open New");
        items.add(r);

        items.add(null);

        r = new GenericData.Record(menuItemSchema);
        r.put("id", "ZoomIn");
        r.put("label", "Zoom In");
        items.add(r);

        r = new GenericData.Record(menuItemSchema);
        r.put("id", "ZoomOut");
        r.put("label", "Zoom Out");
        items.add(r);

        r = new GenericData.Record(menuItemSchema);
        r.put("id", "OriginalView");
        r.put("label", "Original View");
        items.add(r);
        
        items.add(null);

        r = new GenericData.Record(menuItemSchema);
        r.put("id", "Quality");
        r.put("label", null);
        items.add(r);

        r = new GenericData.Record(menuItemSchema);
        r.put("id", "Pause");
        r.put("label", null);
        items.add(r);

        r = new GenericData.Record(menuItemSchema);
        r.put("id", "Mute");
        r.put("label", null);
        items.add(r);

        items.add(null);

        r = new GenericData.Record(menuItemSchema);
        r.put("id", "Find");
        r.put("label", "Find...");
        items.add(r);

        r = new GenericData.Record(menuItemSchema);
        r.put("id", "FindAgain");
        r.put("label", "Find Again");
        items.add(r);

        r = new GenericData.Record(menuItemSchema);
        r.put("id", "Copy");
        r.put("label", null);
        items.add(r);

        r = new GenericData.Record(menuItemSchema);
        r.put("id", "CopyAgain");
        r.put("label", "Copy Again");
        items.add(r);

        r = new GenericData.Record(menuItemSchema);
        r.put("id", "CopySVG");
        r.put("label", "Copy SVG");
        items.add(r);

        r = new GenericData.Record(menuItemSchema);
        r.put("id", "ViewSVG");
        r.put("label", "View SVG");
        items.add(r);

        r = new GenericData.Record(menuItemSchema);
        r.put("id", "ViewSource");
        r.put("label", "View Source");
        items.add(r);

        r = new GenericData.Record(menuItemSchema);
        r.put("id", "SaveAs");
        r.put("label", "Save As");
        items.add(r);

        items.add(null);

        r = new GenericData.Record(menuItemSchema);
        r.put("id", "Help");
        r.put("label", null);
        items.add(r);

        r = new GenericData.Record(menuItemSchema);
        r.put("id", "About");
        r.put("label", "About Adobe CVG Viewer...");
        items.add(r);
        return items;
    }
    
    // ---------------------------------------------------------------
    // Protobuf - 1 message
    // ---------------------------------------------------------------
    static void serializeProtobuf() throws Exception {
        MenuProto.Menu menu = buildProtoItems();
        byte[] protoBytes = menu.toByteArray();
        Files.write(Paths.get("output_proto_1.pb"), protoBytes);
        System.out.println("Protobuf (1 msg): " + protoBytes.length + " bytes");
    }

    // ---------------------------------------------------------------
    // Protobuf - 100 message
    // ---------------------------------------------------------------
    static void serializeProtobuf100() throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        for (int i = 0; i < 100; i++) {
            MenuProto.Menu menu = buildProtoItems();
            menu.writeTo(baos);
        }

        byte[] all = baos.toByteArray();
        Files.write(Paths.get("output_proto_100.pb"), all);
        System.out.println("Protobuf (100 msg): " + all.length + " bytes");
    }

    // ---------------------------------------------------------------
    // Helper: Build MenuItem objects using the generated builder
    // ---------------------------------------------------------------
    static MenuProto.Menu buildProtoItems() {
        MenuProto.Menu menu = MenuProto.Menu.newBuilder()
            .setHeader("SVG Viewer")

            .addItems(MenuProto.ItemOrNull.newBuilder()
            .setHasValue(true)
            .setItem(MenuProto.MenuItem.newBuilder()
                .setId("Open").build())
            .build())

        .addItems(MenuProto.ItemOrNull.newBuilder()
            .setHasValue(true)
            .setItem(MenuProto.MenuItem.newBuilder()
                .setId("OpenNew").setLabel("Open New").build())
            .build())
            
        .addItems(MenuProto.ItemOrNull.newBuilder()
            .setHasValue(false)
            .build())

        .addItems(MenuProto.ItemOrNull.newBuilder()
            .setHasValue(true)
            .setItem(MenuProto.MenuItem.newBuilder()
                .setId("ZoomIn").setLabel("Zoom In").build())
            .build())
        
        .addItems(MenuProto.ItemOrNull.newBuilder()
            .setHasValue(true)
            .setItem(MenuProto.MenuItem.newBuilder()
                .setId("ZoomOut").setLabel("Zoom Out").build())
            .build())

        .addItems(MenuProto.ItemOrNull.newBuilder()
            .setHasValue(true)
            .setItem(MenuProto.MenuItem.newBuilder()
                .setId("OriginalView").setLabel("Original View").build())
            .build())

        .addItems(MenuProto.ItemOrNull.newBuilder()
            .setHasValue(false)
            .build())

        .addItems(MenuProto.ItemOrNull.newBuilder()
            .setHasValue(true)
            .setItem(MenuProto.MenuItem.newBuilder()
                .setId("Quality").build())
            .build())

        .addItems(MenuProto.ItemOrNull.newBuilder()
            .setHasValue(true)
            .setItem(MenuProto.MenuItem.newBuilder()
                .setId("Pause").build())
            .build())

        .addItems(MenuProto.ItemOrNull.newBuilder()
            .setHasValue(true)
            .setItem(MenuProto.MenuItem.newBuilder()
                .setId("Mute").build())
            .build())

        .addItems(MenuProto.ItemOrNull.newBuilder()
            .setHasValue(false)
            .build())

        .addItems(MenuProto.ItemOrNull.newBuilder()
            .setHasValue(true)
            .setItem(MenuProto.MenuItem.newBuilder()
                .setId("Find").setLabel("Find...").build())
            .build())

        .addItems(MenuProto.ItemOrNull.newBuilder()
            .setHasValue(true)
            .setItem(MenuProto.MenuItem.newBuilder()
                .setId("FindAgain").setLabel("Find Again").build())
            .build())

        .addItems(MenuProto.ItemOrNull.newBuilder()
            .setHasValue(true)
            .setItem(MenuProto.MenuItem.newBuilder()
                .setId("Copy").build())
            .build())

        .addItems(MenuProto.ItemOrNull.newBuilder()
            .setHasValue(true)
            .setItem(MenuProto.MenuItem.newBuilder()
                .setId("CopyAgain").setLabel("Copy Again").build())
            .build())

        .addItems(MenuProto.ItemOrNull.newBuilder()
            .setHasValue(true)
            .setItem(MenuProto.MenuItem.newBuilder()
                .setId("CopySVG").setLabel("Copy SVG").build())
            .build())

        .addItems(MenuProto.ItemOrNull.newBuilder()
            .setHasValue(true)
            .setItem(MenuProto.MenuItem.newBuilder()
                .setId("ViewSVG").setLabel("View SVG").build())
            .build())

        .addItems(MenuProto.ItemOrNull.newBuilder()
            .setHasValue(true)
            .setItem(MenuProto.MenuItem.newBuilder()
                .setId("ViewSource").setLabel("View Source").build())
            .build())

        .addItems(MenuProto.ItemOrNull.newBuilder()
            .setHasValue(true)
            .setItem(MenuProto.MenuItem.newBuilder()
                .setId("SaveAs").setLabel("Save As").build())
            .build())

        .addItems(MenuProto.ItemOrNull.newBuilder()
            .setHasValue(false)
            .build())

        .addItems(MenuProto.ItemOrNull.newBuilder()
            .setHasValue(true)
            .setItem(MenuProto.MenuItem.newBuilder()
                .setId("Help").build())
            .build())

        .addItems(MenuProto.ItemOrNull.newBuilder()
            .setHasValue(true)
            .setItem(MenuProto.MenuItem.newBuilder()
                .setId("About").setLabel("About Adobe GVG Viewer...").build())
            .build())

            .build();
        return menu;
    }

    // ---------------------------------------------------------------
    // Thrift - 1 message
    // ---------------------------------------------------------------
    static void serializeThrift() throws Exception {
        Menu menu = buildThriftMenu();
    
        TSerializer serializer = new TSerializer(new TCompactProtocol.Factory());
        byte[] thriftBytes = serializer.serialize(menu);
    
        Files.write(Paths.get("output_thrift_1.thrift"), thriftBytes);
        System.out.println("Thrift (1 msg): " + thriftBytes.length + " bytes");
    }

    // ---------------------------------------------------------------
    // Thrift - 100 messages
    // ---------------------------------------------------------------
    static void serializeThrift100() throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        TSerializer serializer = new TSerializer(new TCompactProtocol.Factory());
    
        for (int i = 0; i < 100; i++) {
            Menu menu = buildThriftMenu();
            byte[] thriftBytes = serializer.serialize(menu);
            baos.write(thriftBytes);
        }
    
        byte[] all = baos.toByteArray();
        Files.write(Paths.get("output_thrift_100.thrift"), all);
        System.out.println("Thrift (100 msg): " + all.length + " bytes");
    }

    // ---------------------------------------------------------------
    // Helper: Build the Thrift Menu with all 22 items
    // ---------------------------------------------------------------
    static Menu buildThriftMenu() {
        List<ItemOrNull> items = new ArrayList<>();

        items.add(new ItemOrNull(true).setItem(new MenuItem().setId("Open")));
        items.add(new ItemOrNull(true).setItem(new MenuItem().setId("OpenNew").setLabel("Open New")));
        items.add(new ItemOrNull(false));                                                                     // null slot
        items.add(new ItemOrNull(true).setItem(new MenuItem().setId("ZoomIn").setLabel("Zoom In")));
        items.add(new ItemOrNull(true).setItem(new MenuItem().setId("ZoomOut").setLabel("Zoom Out")));
        items.add(new ItemOrNull(true).setItem(new MenuItem().setId("OriginalView").setLabel("Original View")));
        items.add(new ItemOrNull(false));                                                                     // null slot
        items.add(new ItemOrNull(true).setItem(new MenuItem().setId("Quality")));
        items.add(new ItemOrNull(true).setItem(new MenuItem().setId("Pause")));
        items.add(new ItemOrNull(true).setItem(new MenuItem().setId("Mute")));
        items.add(new ItemOrNull(false));                                                                     // null slot
        items.add(new ItemOrNull(true).setItem(new MenuItem().setId("Find").setLabel("Find...")));
        items.add(new ItemOrNull(true).setItem(new MenuItem().setId("FindAgain").setLabel("Find Again")));
        items.add(new ItemOrNull(true).setItem(new MenuItem().setId("Copy")));
        items.add(new ItemOrNull(true).setItem(new MenuItem().setId("CopyAgain").setLabel("Copy Again")));
        items.add(new ItemOrNull(true).setItem(new MenuItem().setId("CopySVG").setLabel("Copy SVG")));
        items.add(new ItemOrNull(true).setItem(new MenuItem().setId("ViewSVG").setLabel("View SVG")));
        items.add(new ItemOrNull(true).setItem(new MenuItem().setId("ViewSource").setLabel("View Source")));
        items.add(new ItemOrNull(true).setItem(new MenuItem().setId("SaveAs").setLabel("Save As")));
        items.add(new ItemOrNull(false));                                                                     // null slot
        items.add(new ItemOrNull(true).setItem(new MenuItem().setId("Help")));
        items.add(new ItemOrNull(true).setItem(new MenuItem().setId("About").setLabel("About Adobe CVG Viewer...")));

        return new Menu().setHeader("SVG Viewer").setItems(items);
    }
}