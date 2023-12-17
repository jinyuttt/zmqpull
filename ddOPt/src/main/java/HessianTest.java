import HessianObj.HessianSerialize;
import HessianObj.Person;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class HessianTest {
    public static void main(String[]avg)
    {
         HessianSerialize  test=new HessianSerialize();
        Person  person=new Person();
        person.id=11;
        person.name="ddd";
        person.lst=new ArrayList<>();
        person.lst.add("ssss");
        person.map=new HashMap<>();
        person.map.put("aaa",123);

       byte[]bytes=  test.serialize(person);

      Person p= test.deserialize(bytes,Person.class);

        List<byte[]> lst=new ArrayList<>();
        lst.add(new byte[]{1,2,4});
        lst.add(new byte[]{3,3,5});
       byte[]buf= test.serialize(lst);
      List<byte[]> ll= test.deserialize(buf,List.class);
      System.out.println("s");

    }
}
