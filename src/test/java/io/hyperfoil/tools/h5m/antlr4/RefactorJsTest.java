package io.hyperfoil.tools.h5m.antlr4;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class RefactorJsTest {
    @Test
    public void refactor_arrow_two_parameters(){
        assertEquals("(_one,_two)=>_one+_two",
                RefactorJs.refactor("(one,two)=>one+two",Map.of("one","_one","two","_two")),
                "");
    }
    @Test
    public void refactor_scope_ignore_for(){
        assertEquals("(uno,dos)=>{for (var two=0; two<4; two++){ console.log(uno[two])} return dos}",
                RefactorJs.refactor("(one,two)=>{for (var two=0; two<4; two++){ console.log(one[two])} return two}",Map.of("one","uno","two","dos")),
                "");
    }
    @Test
    public void refactor_scope_ignore_for_of(){
        assertEquals("(uno,dos)=>{for (var two of uno){ console.log(two['one'])} return dos}",
                RefactorJs.refactor("(one,two)=>{for (var two of one){ console.log(two['one'])} return two}",Map.of("one","uno","two","dos")),
                "");
    }
    @Test
    public void refactor_scope_ignore_for_in(){
        assertEquals("(uno,dos)=>{for (var two in uno){ console.log(uno[two])} return dos}",
                RefactorJs.refactor("(one,two)=>{for (var two in one){ console.log(one[two])} return two}",Map.of("one","uno","two","dos")),
                "");
    }
    @Test
    public void refactor_arrow_multi_parameter_single_param_state(){
        assertEquals(
                "(args,foo='yes')=>args.uno",
                RefactorJs.refactor("(args,foo='yes')=>args.one",Map.of("one","uno")),
                "");
    }
    @Test
    public void refactor_arrow_deconstruct_rename_with_default(){
        assertEquals(
                "({uno : rename = 'defaultValue' })=>{ return rename.map(one=>one*2)}",
                RefactorJs.refactor("({one : rename = 'defaultValue' })=>{ return rename.map(one=>one*2)}", Map.of("one","uno")),
                "");
    }
    @Test
    public void refactor_named_function_deconstruct_rename_with_default(){
        assertEquals(
                "function one({uno : rename = 'defaultValue' }){ return rename.map(one=>one*2)}",
                RefactorJs.refactor("function one({one : rename = 'defaultValue' }){ return rename.map(one=>one*2)}", Map.of("one","uno")),
                "");
    }
    @Test @Disabled("not seen in Horreum and not supported by antlr4")
    public void refactor_anonymous_function_deconstruct_rename_with_default(){
        assertEquals(
                "function (uno,rename){ return rename.map(one=>one*2)}",
                RefactorJs.refactor("function (one,rename){ return rename.map(one=>one*2)}", Map.of("one","uno")),
                "");
    }
    @Test
    public void refactor_generator_function_deconstruct(){
        assertEquals(
                "function* dataset({uno : one,dos : two}){ var found=two.map(one=>one*2); for (var two of map){ yield two[one] } }",
                RefactorJs.refactor("function* dataset({one,two}){ var found=two.map(one=>one*2); for (var two of map){ yield two[one] } }",
                        Map.of("one","uno","two","dos"))
        );
    }
    @Test
    public void refactor_generator_function_two_param(){
        assertEquals(
                "function* dataset(uno,dos){ var found=dos.map(one=>one*2); for (var two of map){ yield two[uno] } }",
                RefactorJs.refactor("function* dataset(one,two){ var found=two.map(one=>one*2); for (var two of map){ yield two[one] } }",
                        Map.of("one","uno","two","dos"))
        );
    }
    @Test
    public void refactor_arrow_deconstruct_rename(){
        assertEquals(
                "({uno : rename})=>{ return rename.map(one=>one*2)}",
                RefactorJs.refactor("({one : rename})=>{ return rename.map(one=>one*2)}", Map.of("one","uno")),
                "");
    }
    @Test
    public void refactor_arrow_deconstruct_with_default(){
        assertEquals(
                "({uno = 'defaultValue' })=>{ return rename.map(one=>one*2)}",
                RefactorJs.refactor("({one = 'defaultValue' })=>{ return rename.map(one=>one*2)}", Map.of("one","uno")),
                "");
    }
    @Test
    public void refactor_arrow_single_param_dot_access(){
        assertEquals(
                "(value)=>value.uno*value.other.reduce((one,two)=>one+two)",
                RefactorJs.refactor("(value)=>value.one*value.other.reduce((one,two)=>one+two)", Map.of("one","uno"))
        );
    }
    @Test
    public void refactor_single_param_array_access_single_quote(){
        assertEquals("(value)=>value['uno']*value.other.reduce((one,two)=>one+two)",
                RefactorJs.refactor("(value)=>value['one']*value.other.reduce((one,two)=>one+two)", Map.of("one","uno")));
    }
    @Test
    public void refactor_single_param_array_access_double_quote(){
        assertEquals("(value)=>value[\"uno\"]*value.other.reduce((one,two)=>one+two)",
                RefactorJs.refactor("(value)=>value[\"one\"]*value.other.reduce((one,two)=>one+two)", Map.of("one","uno")));
    }


    @Test
    public void refactor_skip_references_from_other_objects(){
        assertEquals(
                "({uno : one,dos : two})=>{ return one.map(val=>val.one+val.two)}",
                RefactorJs.refactor("({one,two})=>{ return one.map(val=>val.one+val.two)}", Map.of("one","uno","two","dos")),
                "");
    }
    @Test
    public void refactor_spread_parameter_uses_map(){
        assertEquals("({\ne_bar : bar,\nbuz\n})=>{\n  return bar.map(v=>v['bar'])\n}",RefactorJs.refactor("({\nbar,\nbuz\n})=>{\n  return bar.map(v=>v['bar'])\n}",Map.of("bar","e_bar")));
    }
    @Test
    public void refactor_spaced_parameters() {
        assertEquals("function foo( biz , buz ){}", RefactorJs.refactor("function foo( fiz , fuzz ){}", Map.of("fiz", "biz", "fuzz", "buz")));
    }
    @Test
    public void refactor_nested_parameters() {
        assertEquals("function foo({biz : fiz,buz : fuzz}){}", RefactorJs.refactor("function foo({fiz,fuzz}){}", Map.of("fiz", "biz", "fuzz", "buz")));
    }
    @Test
    public void refactor_skip_method_call() {
        assertEquals("buz=>buz.foo()", RefactorJs.refactor("foo=>foo.foo()", Map.of("foo", "buz")));
    }
    @Test
    public void refactor_string_literal() {
        assertEquals("buz=>`${buz}`", RefactorJs.refactor("foo=>`${foo}`", Map.of("foo", "buz")));
    }
    @Test
    public void renameParameter_skip_object_key(){
        assertEquals("buzz=>({foo:buzz})",RefactorJs.refactor("foo=>({foo:foo})",Map.of("foo","buzz")));
    }
    @Test
    public void renameParameter_tertiary_refernece(){
        assertEquals("(_a,_b,_c)=> _a ? _b: _c",RefactorJs.refactor("(a,b,c)=> a ? b: c",Map.of("a","_a","b","_b","c","_c")));
    }
    @Test
    public void renameParameter_filter_miss(){
        assertEquals("value => value === \"true\"",RefactorJs.refactor("value => value === \"true\"",Map.of("before","after")));
    }
    @Test
    public void renameParameter_filter_array_access(){
        assertEquals("value => (value[\"Fun ID\"].match(/^gitlab-ci-nightly/))",RefactorJs.refactor("value => (value[\"Run ID\"].match(/^gitlab-ci-nightly/))",Map.of("Run ID","Fun ID")));
    }
    @Test
    public void renameParameter_filter_property_access(){
        assertEquals("value => value.after",RefactorJs.refactor("value => value.before",Map.of("before","after")));
    }
    
}
