package io.hyperfoil.tools.h5m.antlr4;


import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.ParseTreeWalker;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.antlr.v4.runtime.tree.Trees;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class RefactorJs extends JavaScriptParserBaseListener{


    private enum State {Starting,SingleParam,Destructured,AddRename,RenameVariable}

    private State state = State.Starting;
    private String paramName;
    private TokenStreamRewriter rewriter;
    Map<String,String> renames;
    Map<String, AtomicInteger> scopeDepth;

    private RefactorJs(Map<String,String> renames, TokenStreamRewriter rewriter) {
        this.renames = renames;
        this.rewriter = rewriter;
        this.paramName = "";
        this.scopeDepth = new HashMap<>();
    }

    public String unQuote(String input){
        char firstChar = input.charAt(0);
        if(firstChar == input.charAt(input.length()-1) && "'\"".indexOf(firstChar)>-1){
            input = input.substring(0,input.length()-1);
        }
        return input.replace("\\\\"+firstChar,""+firstChar);//remove quotes
    }
    public String quote(String input,char quoteChar){
        return quoteChar+input.replace(""+quoteChar,"\\\\"+quoteChar)+quoteChar;
    }
    public int scopeDepth(String name){
        scopeDepth.putIfAbsent(name, new AtomicInteger(0));
        return scopeDepth.get(name).get();
    }
    public int increment(String name){

        scopeDepth.putIfAbsent(name, new AtomicInteger(0));
        int rtrn = scopeDepth.get(name).incrementAndGet();
        return rtrn;

    }
    public boolean isCorrectScope(String name){
        return scopeDepth(name) == 1;
    }
    public int decrement(String name){

        if(scopeDepth.containsKey(name)){
            int rtrn = scopeDepth.get(name).decrementAndGet();
            return rtrn;
        }else{
            return 0;
        }
    }

    public static String refactor(String input,Map<String,String> renames){
        var charStream = CharStreams.fromString(input);
        JavaScriptLexer lexer = new JavaScriptLexer(charStream);
        CommonTokenStream tokens = new CommonTokenStream(lexer);
        JavaScriptParser parser = new JavaScriptParser(tokens);

        TokenStreamRewriter rewriter = new TokenStreamRewriter(tokens);
        RefactorJs listener = new RefactorJs(renames,rewriter);
        ParseTreeWalker walker = new ParseTreeWalker();
        ParseTree tree = parser.program();

        walker.walk(listener, tree);
        return rewriter.getText();
    }

    //for { name : rename = defaultValue , name : rename, name = defaultValue, name} deconstruction in function parameter
    private void scanParameterObjectLiteral(JavaScriptParser.ObjectLiteralContext ctx){
        for(JavaScriptParser.PropertyAssignmentContext pctx : ctx.propertyAssignment()){
            //{ name : rename = defaultValue, name : rename }
            if(pctx instanceof JavaScriptParser.PropertyExpressionAssignmentContext peactx){
                // name
                String name = peactx.propertyName().identifierName().identifier().Identifier().getText();
                if(renames.containsKey(name)){
                    increment(name);
                    if(State.Starting == state && isCorrectScope(name)) {
                        state = State.Destructured;
                        rewriter.replace(peactx.propertyName().identifierName().identifier().Identifier().getSymbol(), renames.get(name));
                    }
                }
                JavaScriptParser.SingleExpressionContext sectx = peactx.singleExpression();
                // rename = defaultValue
                if(sectx instanceof JavaScriptParser.AssignmentExpressionContext aectx){
                    List<JavaScriptParser.SingleExpressionContext> sectxs = aectx.singleExpression();
                    if(sectxs.size() == 2){
                        String rename = sectxs.get(0).getText();
                        String defaultValue = sectxs.get(1).getText();
                    }
                // name : rename
                }else if (sectx instanceof JavaScriptParser.IdentifierExpressionContext iectx){
                    String rename = iectx.identifier().Identifier().getText();
                }
            //{ name = defaultValue, name }
            } else if (pctx instanceof JavaScriptParser.PropertyShorthandContext psctx){
                JavaScriptParser.SingleExpressionContext sectx = psctx.singleExpression();
                //{ name = defaultValue }
                if(sectx instanceof JavaScriptParser.AssignmentExpressionContext aectx){
                    List<JavaScriptParser.SingleExpressionContext> sectxs = aectx.singleExpression();
                    //name = defaultValue
                    if(sectxs.size() ==2){
                        String name = sectxs.get(0).getText();
                        String defaultValue = sectxs.get(1).getText();
                        if(renames.containsKey(name)){
                            increment(name);
                            if(State.Starting == state && isCorrectScope(name)){
                                state = State.Destructured;
                                if(sectxs.get(0) instanceof JavaScriptParser.IdentifierExpressionContext iectx){
                                    rewriter.replace(iectx.identifier().Identifier().getSymbol(),renames.get(name));
                                }
                            }
                        }
                    }
                //{ name }
                }else if (sectx instanceof JavaScriptParser.IdentifierExpressionContext iectx){
                    TerminalNode identifier = iectx.identifier().Identifier();
                    String name = identifier.getText();
                    if(renames.containsKey(name)){
                        increment(name);
                        if(State.Starting.equals(state)) {//we can decide what to do
                            state = State.AddRename;
                            rewriter.replace(identifier.getSymbol(), renames.get(name) + " : " + name);
                        }else if (State.AddRename.equals(state)) {
                            rewriter.replace(identifier.getSymbol(), renames.get(name) + " : " + name);
                        }


                    }
                }
            }
        }
    }

    @Override public void enterArrowFunction(JavaScriptParser.ArrowFunctionContext ctx) {
        JavaScriptParser.ArrowFunctionParametersContext afpc = ctx.arrowFunctionParameters();
        if(afpc!=null){
            //multi-parameter arrow function
            if (afpc.formalParameterList() != null) {//I think this is handled by enterFormalParameterArg
                for (JavaScriptParser.FormalParameterArgContext arg : afpc.formalParameterList().formalParameterArg()) {
                    //(one,two)=>...
                    if (arg.assignable() != null && arg.assignable().identifier() != null) {
                        TerminalNode id = arg.assignable().identifier().Identifier();
                        if (id != null && renames.containsKey(id.getText())) {
                            increment(id.getText());
                            if (State.Starting == state || State.RenameVariable == state) {
                                state = State.RenameVariable;
                                rewriter.replace(id.getSymbol(), renames.get(id.getText()));
                            }

                        }
                    }
                }
                if (afpc.formalParameterList().formalParameterArg().size() >= 1 && state == State.Starting) {
                    JavaScriptParser.IdentifierContext id = afpc.formalParameterList().formalParameterArg(0).assignable().identifier();
                    if (id != null) {
                        if (renames.containsKey(id.getText())) {
                            increment(id.getText());
                            state = State.RenameVariable;
                            if (isCorrectScope(id.getText())) {
                                rewriter.replace(id.Identifier().getSymbol(), renames.get(id.getText()));
                            }
                        } else {
                            state = State.SingleParam;
                            paramName = id.getText();
                            increment(paramName);
                        }
                    } else if (afpc.formalParameterList().formalParameterArg(0).assignable() != null && afpc.formalParameterList().formalParameterArg(0).assignable().objectLiteral() != null) {
                        scanParameterObjectLiteral(afpc.formalParameterList().formalParameterArg(0).assignable().objectLiteral());
                    }
                }
            //single simple parameter arrow function (foo)=>...
            }else if (afpc.propertyName() != null) {
                JavaScriptParser.PropertyNameContext pn = afpc.propertyName();
                TerminalNode id = pn.identifierName().identifier().Identifier();
                if(id!=null){
                    increment(id.getText());
                    if(renames.containsKey(id.getText())){
                        if(State.Starting == state){
                            state = State.RenameVariable;
                            if (isCorrectScope(id.getText())) {
                                rewriter.replace(id.getSymbol(), renames.get(id.getText()));
                            }
                        }
                    }else{
                        if(state == State.Starting){
                            state = State.SingleParam;
                            paramName = afpc.getText();
                        }
                    }
                }
                //TODO check if the property is in renames or if we are SingleParaming
            } else {
                if(afpc.equals(afpc.getPayload())){//single param
                    increment(afpc.getText());
                    if(state == State.Starting){
                        state = State.SingleParam;
                        paramName = afpc.getText();
                    }
                }
            }
        }
    }
    @Override public void exitArrowFunction(JavaScriptParser.ArrowFunctionContext ctx) {
        JavaScriptParser.ArrowFunctionParametersContext afpc = ctx.arrowFunctionParameters();
        if(afpc!=null){
            if(afpc.formalParameterList()!=null){
                for (JavaScriptParser.FormalParameterArgContext arg : afpc.formalParameterList().formalParameterArg()) {
                    //(one,two)=>...
                    if (arg.assignable() != null && arg.assignable().identifier() != null) {
                        TerminalNode id = arg.assignable().identifier().Identifier();
                        if (id != null) {
                            decrement(id.getText());
                        }
                    }
                }
            }else{
                if(afpc.equals(afpc.getPayload())){
                    decrement(afpc.getText());
                }
            }
        }
    }
    //TODO enter and exit are identical except for increment vs decrement
    @Override public void enterStatement(JavaScriptParser.StatementContext ctx) {

        JavaScriptParser.IterationStatementContext isctx = ctx.iterationStatement();
        if(isctx!=null){
            //for ( var name of iterable ){...}
            if(isctx instanceof JavaScriptParser.ForOfStatementContext fosc){
                //TODO merge the SingleVariableDeclarationContext from each ForXStatementContext
                JavaScriptParser.SingleVariableDeclarationContext svdc = fosc.singleVariableDeclaration();
                if(svdc.variableDeclaration() != null){
                    String name  = svdc.variableDeclaration().getText();
                    if(renames.containsKey(name) || name.equals(paramName)){
                        increment(name);
                    }
                }
            //for (var key in obj ){...}
            }else if (isctx instanceof JavaScriptParser.ForInStatementContext fisc){
                JavaScriptParser.SingleVariableDeclarationContext svdc = fisc.singleVariableDeclaration();
                if(svdc.variableDeclaration() != null){
                    String name  = svdc.variableDeclaration().getText();
                    if(renames.containsKey(name) || name.equals(paramName)){
                        increment(name);
                    }
                }
            //for (var i =0,k=20; i<k; i++){...}
            }else if (isctx instanceof JavaScriptParser.ForStatementContext fsc){
                if(fsc.variableDeclarationList() !=null){
                    fsc.variableDeclarationList().variableDeclaration().forEach(vdc->{
                        if(vdc.assignable() != null && vdc.assignable().identifier() != null) {
                            TerminalNode id = vdc.assignable().identifier().Identifier();
                            if(id!=null){
                                String name = id.getText();
                                if(renames.containsKey(name) || name.equals(paramName)){
                                    increment(name);
                                }
                            }
                        }
                    });
                }
            }
        }
    }
    @Override public void exitStatement(JavaScriptParser.StatementContext ctx) {
        JavaScriptParser.IterationStatementContext isctx = ctx.iterationStatement();
        if(isctx!=null){
            if(isctx instanceof JavaScriptParser.ForOfStatementContext fosc){
                JavaScriptParser.SingleVariableDeclarationContext svdc = fosc.singleVariableDeclaration();
                if(svdc.variableDeclaration() != null){
                    String name  = svdc.variableDeclaration().getText();
                    if(renames.containsKey(name) || name.equals(paramName)){
                        decrement(name);
                    }
                }
            }else if (isctx instanceof JavaScriptParser.ForInStatementContext fisc){
                JavaScriptParser.SingleVariableDeclarationContext svdc = fisc.singleVariableDeclaration();
                if(svdc.variableDeclaration() != null){
                    String name  = svdc.variableDeclaration().getText();
                    if(renames.containsKey(name) || name.equals(paramName)){
                        decrement(name);
                    }
                }
            }else if (isctx instanceof JavaScriptParser.ForStatementContext fsc){
                if(fsc.variableDeclarationList() !=null){
                    fsc.variableDeclarationList().variableDeclaration().forEach(vdc->{
                        if(vdc.assignable() != null && vdc.assignable().identifier() != null) {
                            TerminalNode id = vdc.assignable().identifier().Identifier();
                            if(id!=null){
                                String name = id.getText();
                                if(renames.containsKey(name) || name.equals(paramName)){
                                    decrement(name);
                                }
                            }
                        }
                    });
                }
            }
        }
    }
    @Override public void enterFunctionDeclaration(JavaScriptParser.FunctionDeclarationContext ctx) {
        JavaScriptParser.FormalParameterListContext fplc = ctx.formalParameterList();
        if(fplc!=null){
            fplc.formalParameterArg().forEach(fpac->{
                if (fpac.assignable() != null && fpac.assignable().identifier() != null) {
                    TerminalNode id = fpac.assignable().identifier().Identifier();
                    if (id != null ) {
                        if(renames.containsKey(id.getText())){
                            increment(id.getText());
                            if(State.Starting == state || state == State.RenameVariable){
                                state = State.RenameVariable;
                                rewriter.replace(id.getSymbol(), renames.get(id.getText()));
                            }
                        }
                    }
                    // if the param is an object
                }else if (fpac.assignable() != null && fpac.assignable().objectLiteral() != null) {
                    scanParameterObjectLiteral(fpac.assignable().objectLiteral());
                }
            });
        }
    }
    @Override public void exitFunctionDeclaration(JavaScriptParser.FunctionDeclarationContext ctx) {
        JavaScriptParser.FormalParameterListContext fplc = ctx.formalParameterList();
        if(fplc!=null){
            fplc.formalParameterArg().forEach(fpac->{
                if (fpac.assignable() != null && fpac.assignable().identifier() != null) {
                    TerminalNode id = fpac.assignable().identifier().Identifier();
                    if (id != null ) {
                        if(renames.containsKey(id.getText())){
                            decrement(id.getText());
                        }
                    }
                    // if the param is an object
                }else if (fpac.assignable() != null && fpac.assignable().objectLiteral() != null) {
                    //scanParameterObjectLiteral(fpac.assignable().objectLiteral());
                    //TODO implement decrement equivalent for scanParameterObjectLiteral
                }
            });
        }
    }
    @Override public void enterFunctionExpression(JavaScriptParser.FunctionExpressionContext ctx) {
        if(ctx.anonymousFunction() != null){
            JavaScriptParser.AnonymousFunctionContext anonfc = ctx.anonymousFunction();
            if(anonfc.getPayload() instanceof JavaScriptParser.ArrowFunctionContext afc){

            }
        }
    }

    @Override public void enterMemberDotExpression(JavaScriptParser.MemberDotExpressionContext ctx) {
        String memberName = ctx.singleExpression().getText();
        //String memberName = ctx.identifierName().identifier().Identifier().getText();

        RuleContext methodCalled = ctx.identifierName().getRuleContext();
        if(State.SingleParam == state && memberName.equals(paramName) && isCorrectScope(memberName) && renames.containsKey(methodCalled.getText())){
            rewriter.replace(ctx.identifierName().identifier().Identifier().getSymbol(), renames.get(methodCalled.getText()));
        }else if (State.RenameVariable == state && renames.containsKey(memberName) && isCorrectScope(memberName) ){
            if(ctx.singleExpression() instanceof JavaScriptParser.IdentifierExpressionContext iectx){
                rewriter.replace(iectx.identifier().Identifier().getSymbol(), renames.get(iectx.getText()));
            }
        }
    }

    @Override
    public void enterIdentifierExpression(JavaScriptParser.IdentifierExpressionContext ctx) {
        TerminalNode id = ctx.identifier().Identifier();
        //TODO State.RenamingVariable
        if(State.RenameVariable.equals(state) && renames.containsKey(id.getText())){
            if(isCorrectScope(id.getText())){
                rewriter.replace(id.getSymbol(), renames.get(id.getText()));
            }
        }
    }
    @Override public void enterMemberIndexExpression(JavaScriptParser.MemberIndexExpressionContext ctx) {
        if(state==State.SingleParam && paramName.equals(ctx.singleExpression().getText())){
            if(ctx.expressionSequence().singleExpression()!=null && ctx.expressionSequence().singleExpression().size()==1){
                JavaScriptParser.SingleExpressionContext expressionContext = ctx.expressionSequence().singleExpression().get(0);
                if(expressionContext instanceof JavaScriptParser.LiteralExpressionContext lectx){
                    if(lectx.literal()!=null){
                        if(lectx.literal().StringLiteral()!=null){
                            String literalValue = lectx.literal().StringLiteral().getText();
                            String quoteChar = literalValue.substring(0,1);
                            literalValue = literalValue.substring(1,literalValue.length()-1).replaceAll("\\\\"+quoteChar,quoteChar);
                            if(renames.containsKey(literalValue)){
                                String newName = renames.get(literalValue);
                                if(newName.contains(quoteChar)){
                                    newName = newName.replace(quoteChar,"\\\\"+quoteChar);
                                }
                                rewriter.replace(lectx.literal().StringLiteral().getSymbol(),quote(newName,quoteChar.charAt(0)));
                            }
                        }
                    }
                }
            }else{

            }
        }
    }
}
