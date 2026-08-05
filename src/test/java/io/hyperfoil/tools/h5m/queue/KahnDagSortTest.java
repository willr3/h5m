package io.hyperfoil.tools.h5m.queue;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class KahnDagSortTest {

    private class Item {
        private String name;
        private List<Item> dependencies;
        public Item(String name){
            this.name=name;
            this.dependencies=new ArrayList<>();
        }
        public Item(String name,Item...dependencies){
            this.name=name;
            this.dependencies= Arrays.asList(dependencies);
        }
        public List<Item> getDependencies(){return this.dependencies;}
        @Override
        public String toString(){
            return name;
        }
        @Override
        public boolean equals(Object o) {
            if(o instanceof Item e){
                return Objects.equals(name,e.name) && Objects.equals(dependencies,e.dependencies);
            }
            return false;
        }
        @Override
        public int hashCode(){
            return Objects.hash(name,dependencies);
        }
    }
    @Test
    public void sort_and_preserve_order(){
        Item a = new Item("a");
        Item b = new Item("b");
        Item c = new Item("c");
        Item ab = new Item("ab",a,b);
        Item abc = new Item("abc",a,b,c);

        List<Item> input = List.of(abc,ab,c,b,a);

        List<Item> sorted = KahnDagSort.sort(input,Item::getDependencies);

        assertTrue(sorted.indexOf(a) < sorted.indexOf(ab),"a should be before ab: "+sorted);
        assertTrue(sorted.indexOf(a) < sorted.indexOf(abc),"a should be after abc: "+sorted );
        assertTrue(sorted.indexOf(b) < sorted.indexOf(ab),"b should be after ab: "+sorted);
        assertTrue(sorted.indexOf(b) < sorted.indexOf(abc),"b should be after abc: "+sorted);
        assertTrue(sorted.indexOf(c) < sorted.indexOf(abc),"c should be after abc: "+sorted);

        //preserve order of non-dependant items
        assertTrue(sorted.indexOf(c) < sorted.indexOf(a),"c should be before a to preserve add order: "+sorted);
        assertTrue(sorted.indexOf(c) < sorted.indexOf(b),"c should be before b to preserve add order: "+sorted);
        assertTrue(sorted.indexOf(b) < sorted.indexOf(a),"b should be before a to preserve add order: "+sorted);
    }

    @Test
    public void sort_duplicate_entries(){
        Item a = new Item("a");
        Item b = new Item("b");
        Item c = new Item("c");
        Item ab = new Item("ab",a,b);
        Item ab2 = new Item("ab",a,b);
        Item abc = new Item("abc",a,b,c);

        assertEquals(ab,ab2);
        assertEquals(ab2,ab);
        assertEquals(ab.hashCode(),ab2.hashCode());

        List<Item> input = List.of(abc,ab,c,b,ab2,a);

        List<Item> sorted = KahnDagSort.sort(input,Item::getDependencies);

        assertEquals(input.size(),sorted.size());
    }
}

