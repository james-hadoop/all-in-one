package com.james.protobuf;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

import com.james.protobuf.entity.AddressBookProtos.Person;

public class Demo1 {

    public static void main(String[] args) throws IOException {
        // build a object with protobuf
        // protoc --java_out=. addressbook.proto
        Person john = Person.newBuilder().setId(1234).setName("John Doe").setEmail("jdoe@example.com")
                .addPhones(Person.PhoneNumber.newBuilder().setNumber("555-4321").setType(Person.PhoneType.HOME))
                .build();

        // write object
        FileOutputStream fos = new FileOutputStream("data/protobuf/john.obj");
        john.writeTo(fos);

        // read object
        Person john2 = Person.parseFrom(new FileInputStream("data/protobuf/john.obj"));
        System.out.println(john2.getId() + "\n" + john2.getName() + "\n" + john2.getEmail());
    }
}
