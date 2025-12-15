package exp.command;

import com.fasterxml.jackson.databind.node.DoubleNode;
import com.fasterxml.jackson.databind.node.NumericNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.jayway.jsonpath.internal.filter.ValueNodes;
import io.hyperfoil.tools.yaup.AsciiArt;
import picocli.CommandLine;

import java.util.*;
import java.util.concurrent.Callable;
import java.util.function.Function;
import java.util.stream.IntStream;
import java.util.stream.Stream;

@CommandLine.Command(name="list", aliases = {"show","ls"}, description = "list entities", mixinStandardHelpOptions = true, subcommands={ListFolder.class, ListNode.class, ListValue.class})
public class ListCmd implements Callable<Integer> {

    /*
      HTL HBH HTI HBH HTR
      HBV  h  HCS  h  HBV
      TTL TTH TTI TTH TTR
      TBV  v  TCS  v  TBV
      RSL RSH RSI RSH RSR
      TBV  v  TBS  v  TBV
      TBL TBH TBI TBH TBR

      minimum requirements are HORIZONTAL, VERTICAL, and INTERSECT
      HORIZONTAL -> TTH
      VERTICAL -> HCS, TCS
      INTERSECT -> TTI
     */

    public static final String HEADER_TOP_LEFT = "HTL";
    public static final String HEADER_TOP_RIGHT = "HTR";
    public static final String HEADER_TOP_INTERSECT = "HTI";
    public static final String HEADER_COLUMN_SEPARATOR = "HCS";
    public static final String HEADER_BORDER_VERTICAL = "HBV";
    public static final String HEADER_BORDER_HORIZONTAL = "HBH";
    public static final String TABLE_TOP_LEFT = "TTL";
    public static final String TABLE_TOP_RIGHT = "TTR";
    public static final String TABLE_TOP_HORIZONTAL= "TTH";
    public static final String TABLE_TOP_INTERSECT = "TTI";
    public static final String TABLE_BORDER_VERTICAL = "TBV";
    public static final String TABLE_BORDER_HORIZONTAL = "TBH";
    public static final String TABLE_COLUMN_SEPARATOR = "TCS";
    public static final String ROW_SEPARATOR_LEFT = "RSL";
    public static final String ROW_SEPARATOR_HORIZONTAL = "RSH";
    public static final String ROW_SEPARATOR_INTERSECT = "RSI";
    public static final String ROW_SEPARATOR_RIGHT = "RSR";
    public static final String TABLE_BOTTOM_LEFT = "TBL";
    public static final String TABLE_BOTTOM_RIGHT = "TBR";
    public static final String TABLE_BOTTOM_INTERSECT = "TBI";
    public static final String TABLE_BOTTOM_HORIZONTAL = "TBH";


    public static final String VERTICAL = "V";
    public static final String HORIZONTAL = "H";
    public static final String INTERSECT = "X";

    public static final String LEFT_T = "LT";
    public static final String RIGHT_T = "RT";

    public static final Map<String,String> POSTGRES_TABLE=Map.ofEntries(
            Map.entry(VERTICAL,"|"),Map.entry(HORIZONTAL,"-"),Map.entry(INTERSECT,"+")
    );
    public static final Map<String,String> SQLITE_TABLE=
            convertToFullNames(
                Map.ofEntries(
                    Map.entry(VERTICAL,"|"),
                    Map.entry(HORIZONTAL,"-"),
                    Map.entry(INTERSECT,"+")
                ),true
            );
    public static final Map<String,String> DUCKDB_TABLE=convertToFullNames(
            Map.ofEntries(
                // top of outside border
                Map.entry(HEADER_TOP_LEFT,"┌"),Map.entry(HEADER_TOP_RIGHT,"┐"),Map.entry(HEADER_TOP_INTERSECT,"┬"),
                // bottom of outside border
                Map.entry(TABLE_BOTTOM_LEFT,"└"),Map.entry(TABLE_BOTTOM_RIGHT,"┘"),Map.entry(TABLE_BOTTOM_INTERSECT,"┴"),
                // left and right of outside border
                Map.entry(TABLE_TOP_LEFT,"├"),Map.entry(TABLE_TOP_RIGHT,"┤"),
                //inside crosses
                Map.entry(VERTICAL,"│"),Map.entry(HORIZONTAL,"─"),Map.entry(INTERSECT,"┼")
            ),true
    );

    public static final Map<String,String> DOUBLE_TABLE=templateToMap(
        """
        ╔═╦═╗
        ║h║h║
        ╠═╬═╣
        ║v║v║
        ╟─╫─╣
        ╚═╩═╝
        """,DUCKDB_TABLE
    );

    public static Map<String,String> templateToMap(String template,Map<String,String> defaultMap){
        if(template==null || template.isEmpty()){
            return new HashMap<>(defaultMap);
        }
        String split[] = template.split(System.lineSeparator());

        if(Stream.of(split).anyMatch(s->s.length()!=5)){
            return defaultMap;
        }

        if(split.length < 3){
            return defaultMap;
        }
        Map<String,String> map = new HashMap<>();
        if(split.length == 3){
            String row = split[0];
            map.put(HEADER_BORDER_VERTICAL,row.charAt(0)+"");
            map.put(HEADER_COLUMN_SEPARATOR,row.charAt(2)+"");
            row = split[1];
            map.put(TABLE_TOP_LEFT,row.charAt(0)+"");
            map.put(TABLE_TOP_HORIZONTAL,row.charAt(1)+"");
            map.put(TABLE_TOP_INTERSECT,row.charAt(2)+"");
            map.put(TABLE_TOP_RIGHT,row.charAt(4)+"");
            row = split[2];
            map.put(TABLE_BORDER_VERTICAL,row.charAt(0)+"");
            map.put(TABLE_COLUMN_SEPARATOR,row.charAt(2)+"");
        }else if (split.length == 5){
            String line = split[0];
            map.put(HEADER_TOP_LEFT,line.charAt(0)+"");
            map.put(HEADER_BORDER_HORIZONTAL,line.charAt(1)+"");
            map.put(HEADER_TOP_INTERSECT,line.charAt(2)+"");
            map.put(HEADER_TOP_RIGHT,line.charAt(4)+"");
            line = split[1];
            map.put(HEADER_BORDER_VERTICAL,line.charAt(0)+"");
            map.put(HEADER_COLUMN_SEPARATOR,line.charAt(2)+"");
            line = split[2];
            map.put(TABLE_TOP_LEFT,line.charAt(0)+"");
            map.put(TABLE_TOP_HORIZONTAL,line.charAt(1)+"");
            map.put(TABLE_TOP_INTERSECT,line.charAt(2)+"");
            map.put(TABLE_TOP_RIGHT,line.charAt(4)+"");
            line = split[3];
            map.put(TABLE_BORDER_VERTICAL,line.charAt(0)+"");
            map.put(TABLE_COLUMN_SEPARATOR,line.charAt(2)+"");
            line = split[4];
            map.put(TABLE_BOTTOM_LEFT,line.charAt(0)+"");
            map.put(TABLE_BORDER_HORIZONTAL,line.charAt(1)+"");
            map.put(TABLE_BOTTOM_INTERSECT,line.charAt(2)+"");
            map.put(TABLE_BOTTOM_RIGHT,line.charAt(4)+"");
        }else if (split.length == 6){
            String line = split[0];
            map.put(HEADER_TOP_LEFT,line.charAt(0)+"");
            map.put(HEADER_BORDER_HORIZONTAL,line.charAt(1)+"");
            map.put(HEADER_TOP_INTERSECT,line.charAt(2)+"");
            map.put(HEADER_TOP_RIGHT,line.charAt(4)+"");
            line = split[1];
            map.put(HEADER_BORDER_VERTICAL,line.charAt(0)+"");
            map.put(HEADER_COLUMN_SEPARATOR,line.charAt(2)+"");
            line = split[2];
            map.put(TABLE_TOP_LEFT,line.charAt(0)+"");
            map.put(TABLE_TOP_HORIZONTAL,line.charAt(1)+"");
            map.put(TABLE_TOP_INTERSECT,line.charAt(2)+"");
            map.put(TABLE_TOP_RIGHT,line.charAt(4)+"");
            line = split[3];
            map.put(TABLE_BORDER_VERTICAL,line.charAt(0)+"");
            map.put(TABLE_COLUMN_SEPARATOR,line.charAt(2)+"");
            line = split[4];
            map.put(ROW_SEPARATOR_LEFT,line.charAt(0)+"");
            map.put(ROW_SEPARATOR_HORIZONTAL,line.charAt(1)+"");
            map.put(ROW_SEPARATOR_INTERSECT,line.charAt(2)+"");
            map.put(ROW_SEPARATOR_RIGHT,line.charAt(4)+"");
            line =split[5];
            map.put(TABLE_BOTTOM_LEFT,line.charAt(0)+"");
            map.put(TABLE_BORDER_HORIZONTAL,line.charAt(1)+"");
            map.put(TABLE_BOTTOM_INTERSECT,line.charAt(2)+"");
            map.put(TABLE_BOTTOM_RIGHT,line.charAt(4)+"");
        }else {
            return defaultMap;
        }


        return map;
    }

    public static Map<String,String> prefix(String prefix,Map<String,String> map){
        return Map.ofEntries(map.entrySet().stream().map(entry->Map.entry(entry.getKey(),prefix+entry.getValue()+AsciiArt.ANSI_RESET)).toList().toArray(new Map.Entry[]{}));
    }

    private static Map<String,String> convertToFullNames(Map<String,String> map,boolean border){
        Map<String,String> rtrn =  new HashMap<>(map);
        if(rtrn.containsKey(VERTICAL)){
            String v = rtrn.get(VERTICAL);
            rtrn.remove(VERTICAL);
            rtrn.putIfAbsent(HEADER_COLUMN_SEPARATOR,v);
            rtrn.putIfAbsent(TABLE_COLUMN_SEPARATOR,v);
            if(border){
                rtrn.putIfAbsent(HEADER_BORDER_VERTICAL,v);
                rtrn.putIfAbsent(TABLE_BORDER_VERTICAL,v);
            }
        }
        if(rtrn.containsKey(HORIZONTAL)){
            String h = rtrn.get(HORIZONTAL);
            rtrn.remove(HORIZONTAL);
            rtrn.putIfAbsent(TABLE_TOP_HORIZONTAL,h);
            if(border){
                rtrn.putIfAbsent(HEADER_BORDER_HORIZONTAL,h);
                rtrn.putIfAbsent(TABLE_BORDER_HORIZONTAL,h);
            }
        }
        if(rtrn.containsKey(INTERSECT)){
            String i = rtrn.get(INTERSECT);
            rtrn.remove(INTERSECT);
            rtrn.putIfAbsent(TABLE_TOP_INTERSECT,i);
            if(border){
                rtrn.putIfAbsent(HEADER_TOP_LEFT,i);
                rtrn.putIfAbsent(HEADER_TOP_RIGHT,i);
                rtrn.putIfAbsent(HEADER_TOP_INTERSECT,i);
                rtrn.putIfAbsent(TABLE_TOP_LEFT,i);
                rtrn.putIfAbsent(TABLE_TOP_RIGHT,i);
                rtrn.putIfAbsent(TABLE_TOP_INTERSECT,i);
                rtrn.putIfAbsent(TABLE_BOTTOM_LEFT,i);
                rtrn.putIfAbsent(TABLE_BOTTOM_RIGHT,i);
                rtrn.putIfAbsent(TABLE_BOTTOM_INTERSECT,i);
            }
        }
        return rtrn;
    }

    public static boolean hasOutsideBorder(Map<String,String> characters){
        return Stream.of(HEADER_TOP_LEFT, HEADER_BORDER_HORIZONTAL,HEADER_TOP_INTERSECT, HEADER_TOP_RIGHT,
                HEADER_BORDER_VERTICAL, HEADER_COLUMN_SEPARATOR,
                TABLE_TOP_LEFT, TABLE_TOP_HORIZONTAL,TABLE_TOP_INTERSECT,TABLE_TOP_RIGHT,
                TABLE_BORDER_VERTICAL,TABLE_COLUMN_SEPARATOR,
                TABLE_BOTTOM_LEFT, TABLE_BOTTOM_HORIZONTAL, TABLE_BOTTOM_INTERSECT, TABLE_BOTTOM_RIGHT ).allMatch(characters::containsKey);
    }
    public static boolean hasRowSeparator(Map<String,String> characters){
        return Stream.of(ROW_SEPARATOR_LEFT,ROW_SEPARATOR_HORIZONTAL,ROW_SEPARATOR_INTERSECT,ROW_SEPARATOR_RIGHT).allMatch(characters::containsKey);
    }
    public static boolean isValid(Map<String,String> characters){
        return Stream.of(HEADER_COLUMN_SEPARATOR, TABLE_TOP_HORIZONTAL, TABLE_TOP_INTERSECT,TABLE_COLUMN_SEPARATOR).allMatch(characters::containsKey);
    }

    /**
     * splits the row of input strings by line separator and returns a list of top aligned rows for each line in the headers.
     * [ "foo\nfoo","bar" ] -> [ [ "foo","bar" ] , [ "foo","" ] ]
     * @param input
     * @return
     */
    public static List<List<String>> lineSplit(List<String> input){
        List<List<String>> split = input.stream().map(line-> List.of(line.split(System.lineSeparator()))).toList();
        int maxRows = split.stream().mapToInt(List::size).max().orElse(1);
        List<List<String>> rtrn = new ArrayList<>();
        for(int i=0; i<maxRows; i++){
            ArrayList<String> row = new ArrayList<>();
            for(List<String> splitRow : split){
                if(splitRow.size()>i){
                    row.add(splitRow.get(i));
                }else{
                    row.add("");
                }
            }
            rtrn.add(row);
        }
        return rtrn;
    }


    @CommandLine.Parameters(index="0",arity="0..1")
    public String name;

    @Override
    public Integer call() throws Exception {
        CommandLine cmd = new CommandLine(this);
        cmd.usage(System.out);
        return 0;
    }

    public static <T> String table(int maxWidth, List<T> values, Map<String,Function<T,Object>> columns){
        return table(maxWidth,values,List.copyOf(columns.keySet()),List.copyOf(columns.values()));
    }
    public static <T> String table(int maxWidth, List<T> values, List<String> headers, List<Function<T,Object>> accessors){
        Map<String,String> table = DUCKDB_TABLE;
        return table(maxWidth,values,headers,List.copyOf(accessors),H5m.consoleAttached() ? prefix(AsciiArt.ANSI_DARK_GREY,table) : table);
    }

    //creates a text table supporting multi-line headers but not multi-line values
    public static <T> String table(int maxWidth, List<T> values, List<String> headers, List<Function<T,Object>> accessors,Map<String,String> characters){
        characters = isValid(characters) ? characters : SQLITE_TABLE;
        boolean outsideBorder = hasOutsideBorder(characters);
        List<List<String>> headerRows = lineSplit(headers);
        int columnCount = Math.min(headers.size(),accessors.size());
        List<Object[]> rows = new ArrayList<>();

        int[] columnWidths = headers.isEmpty() ? new int[columnCount] : headers.stream().mapToInt(header->Stream.of(header.split(System.lineSeparator())).mapToInt(String::length).max().orElse(0)).toArray();

        String[] columnFormats = new String[columnCount];

        for(int vIdx=0; vIdx<values.size(); vIdx++){
            T value = values.get(vIdx);
            List<Object> rowCells = new ArrayList<>();
            for(int a=0; a<columnCount; a++){
                Object c = accessors.get(a).apply(value);
                if( c instanceof TextNode){
                    c = ((TextNode)c).textValue();
                }else if (c instanceof NumericNode){
                    c = ((NumericNode) c).numberValue();
                }
                if( c == null){
                    c = "NULL";
                }else if( c instanceof Long || c instanceof Integer){
                    if(columnFormats[a]==null){
                        columnFormats[a] = "d";
                    }
                }else if (c instanceof Double || c instanceof Float){
                    if(columnFormats[a]==null || columnFormats[a].equals("d")){
                        columnFormats[a] = "f";
                    }else if (columnFormats[a].equals("s")){
                        //mixed value type colunn, convert to fixed width?
                        c = String.format("%.2f", c);
                    }
                }else{
                    columnFormats[a] = "s";
                }
                int cellWidth = columnFormats[a] == null ? "NULL".length() : switch(columnFormats[a]){
                    case "f" -> String.format("%.2f",c).length();
                    default -> c.toString().length();
                };
                if(cellWidth > columnWidths[a]){
                    columnWidths[a] = cellWidth;
                }
                rowCells.add(c);
            }
            rows.add(rowCells.toArray());
        }
        for(int i=0; i<columnFormats.length; i++){
            if(columnFormats[i]==null){
                columnFormats[i] = "s";
            }
        }
//        int widthSum = IntStream.of(columnWidths).sum();
//        if(widthSum > maxWidth-3*(columnCount-1) ){
//            for(int i=0; i<columnCount; i++){
//                columnWidths[i] = Math.floorDiv(columnWidths[i],maxWidth-3*(columnCount-1));
//            }
//            for(Object[] row : rows){
//                for(int i=0; i<columnCount; i++){
//                    if(columnFormats[i].equals("s") && row[i].toString().length() > columnWidths[i]){
//                        row[i] = row[i].toString().substring(0,columnWidths[i]-1)+ AsciiArt.ELLIPSIS;
//                    }
//                }
//            }
//        }
        StringBuilder topBorderFormat = new StringBuilder();
        StringBuilder bottomBorderFormat = new StringBuilder();
        StringBuilder headerFormat = new StringBuilder();
        StringBuilder rowFormat = new StringBuilder();

        if(outsideBorder){
            topBorderFormat.append(String.format("%s ",characters.get(HEADER_TOP_LEFT)));
            bottomBorderFormat.append(String.format("%s ",characters.get(TABLE_BOTTOM_LEFT)));
            headerFormat.append(String.format("%s ",characters.get(HEADER_BORDER_VERTICAL)));
            rowFormat.append(String.format("%s ",characters.get(TABLE_BORDER_VERTICAL)));
        }

        for(int i=0; i<columnCount; i++){
            if(i>0){
                topBorderFormat.append(String.format(" %s ",characters.get(HEADER_TOP_INTERSECT)));
                bottomBorderFormat.append(String.format(" %s ",characters.get(TABLE_BOTTOM_INTERSECT)));
                headerFormat.append(String.format(" %s ",characters.get(HEADER_BORDER_VERTICAL)));
                rowFormat.append(String.format(" %s ",characters.get(TABLE_COLUMN_SEPARATOR)));
            }
            int width = columnWidths[i];
            if(columnFormats[i]==null){
                columnFormats[i] = "s";
            }
            String format = columnFormats[i];

            headerFormat.append("%" + width + "s");
            topBorderFormat.append("%" + width + "s");
            bottomBorderFormat.append("%" + width + "s");
            switch (format) {
                case "d":
                    rowFormat.append("%" + width + "d");
                    break;
                case "f":
                    rowFormat.append("%" + (width ) + ".2f");
                    break;
                default:
                    rowFormat.append("%-" + width + "s");
            }
        }

        if(outsideBorder){
            topBorderFormat.append(String.format(" %s",characters.get(HEADER_TOP_RIGHT)));
            bottomBorderFormat.append(String.format(" %s",characters.get(TABLE_BOTTOM_RIGHT)));
            headerFormat.append(String.format(" %s",characters.get(HEADER_BORDER_VERTICAL)));
            rowFormat.append(String.format(" %s",characters.get(TABLE_BORDER_VERTICAL)));
        }

        topBorderFormat.append("%n");
        bottomBorderFormat.append("%n");
        headerFormat.append("%n");
        rowFormat.append("%n");
        StringBuilder rtrn = new StringBuilder();
        if(outsideBorder){
            rtrn.append( String.format(topBorderFormat.toString(), Collections.nCopies(columnCount, "").toArray())
                    .replace(" ",characters.get(HEADER_BORDER_HORIZONTAL))

            );
        }
        if(!headers.isEmpty()) {
            for(int r=0; r<headerRows.size(); r++){
                List<String> headerRow = headerRows.get(r);
                StringBuilder row = new StringBuilder();
                if(outsideBorder){
                    row.append(characters.get(HEADER_BORDER_VERTICAL));
                    row.append(" ");
                }
                for(int c=0; c<columnCount; c++){
                    int width = columnWidths[c];
                    String header = headerRow.get(c);
                    int leftPad = (width-header.length())/2;
                    int remainder = width-header.length()-leftPad;
                    if(c>0){
                        row.append(" ");
                        row.append(characters.get(HEADER_COLUMN_SEPARATOR));
                        row.append(" ");
                    }
                    if(leftPad>0){
                        row.append(String.format("%"+leftPad+"s",""));
                    }
                    row.append(header);
                    if(remainder>0){
                        row.append(String.format("%"+remainder+"s",""));
                    }
                }
                if(outsideBorder){
                    row.append(" ");
                    row.append(characters.get(HEADER_BORDER_VERTICAL));
                }
                rtrn.append(row.toString());
                rtrn.append(System.lineSeparator());
            }
            //single row version
            //rtrn.append(String.format(headerFormat.toString(), headers.toArray()));
            //border under the header row
            if(outsideBorder){
                rtrn.append(
                        String.format(
                                        topBorderFormat.toString(),
                                        Collections.nCopies(columnCount, "").toArray()
                                )
                                .replace(" ",characters.get(TABLE_TOP_HORIZONTAL))
                                .replace(characters.get(HEADER_TOP_LEFT),characters.get(TABLE_TOP_LEFT))
                                .replace(characters.get(HEADER_TOP_RIGHT),characters.get(TABLE_TOP_RIGHT))
                                .replace(characters.get(HEADER_TOP_INTERSECT),characters.get(TABLE_TOP_INTERSECT))
                );
            }else{
                rtrn.append(String.format(headerFormat.toString(), Collections.nCopies(columnCount, "").toArray())
                        .replace(" ",characters.get(TABLE_TOP_HORIZONTAL))
                        .replace(characters.get(HEADER_COLUMN_SEPARATOR),characters.get(TABLE_COLUMN_SEPARATOR)));
            }


        }
        for(int i=0; i<rows.size(); i++){
            Object[] row = rows.get(i);
            rtrn.append(String.format(rowFormat.toString(), row ));
            if(i<rows.size()-1 && hasRowSeparator(characters)){
                rtrn.append(characters.get(ROW_SEPARATOR_LEFT));
                rtrn.append(characters.get(ROW_SEPARATOR_HORIZONTAL));
                for(int c=0; c<columnCount; c++){
                    int width = columnWidths[c];
                    if(c>0){
                        rtrn.append(characters.get(ROW_SEPARATOR_HORIZONTAL));
                        rtrn.append(characters.get(ROW_SEPARATOR_INTERSECT));
                        rtrn.append(characters.get(ROW_SEPARATOR_HORIZONTAL));
                    }
                    rtrn.append(characters.get(ROW_SEPARATOR_HORIZONTAL).repeat(width));
                }
                rtrn.append(characters.get(ROW_SEPARATOR_HORIZONTAL));
                rtrn.append(characters.get(ROW_SEPARATOR_RIGHT));
                rtrn.append(System.lineSeparator());
            }
        }
        if(outsideBorder){
            rtrn.append( String.format(topBorderFormat.toString(), Collections.nCopies(columnCount, "").toArray())
                    .replace(characters.get(HEADER_TOP_LEFT),characters.get(TABLE_BOTTOM_LEFT))
                    .replace(characters.get(HEADER_TOP_RIGHT),characters.get(TABLE_BOTTOM_RIGHT))
                    .replace(characters.get(HEADER_TOP_INTERSECT),characters.get(TABLE_BOTTOM_INTERSECT))
                    .replace(" ",characters.get(TABLE_BORDER_HORIZONTAL))

            );
        }

        return rtrn.toString();
    }

}
