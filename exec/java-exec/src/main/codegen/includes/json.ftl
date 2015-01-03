<#-- Licensed to the Apache Software Foundation (ASF) under one or more contributor
  license agreements. See the NOTICE file distributed with this work for additional
  information regarding copyright ownership. The ASF licenses this file to
  You under the Apache License, Version 2.0 (the "License"); you may not use
  this file except in compliance with the License. You may obtain a copy of
  the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required
  by applicable law or agreed to in writing, software distributed under the
  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS
  OF ANY KIND, either express or implied. See the License for the specific
  language governing permissions and limitations under the License. -->
  
// white space 
<IN_JSON> SKIP: {
        " "
    |   "\t"
    |   "\n"
    |   "\r"
    |   "\f"
}
// comments: not really part of JSON spec, but parser shouldn't blow-up if present
<IN_JSON> SKIP: {
    <JS_COMMENT_LINE: "//" (~["\n","\r"])* ("\n"|"\r"|"\r\n")>
}
<IN_JSON> SKIP: {
    <JS_COMMENT_BLOCK: "/*" (~["*"])* "*" ("*" | (~["*","/"] (~["*"])* "*"))* "/">
}

// JSON reserved keywords (prefix with K_ to avoid naming conflicts): only lower case allowed!
<IN_JSON> TOKEN: {
        <JS_TRUE: "true">
    |   <JS_FALSE: "false">
    |   <JS_NULL: "null">
}

// JSON operators (prefix with O_ to avoid naming conflicts)
<IN_JSON> TOKEN: {
        <JS_OPENBRACE: "{"> { intoMap(); }
    |   <JS_CLOSEBRACE: "}"> { outOfMap(); }
    |   <JS_OPENBRACKET: "[">
    |   <JS_CLOSEBRACKET: "]">
    |   <JS_COMMA: ",">
    |   <JS_COLON: ":">
    |   <JS_DOT: ".">
    |   <JS_PLUS: "+">
    |   <JS_MINUS: "-">  
}

// numeric literals
<IN_JSON> TOKEN: {
        <JS_DIGIT: ["0" - "9"] >
    |   <JS_NONZERO_DIGIT: ["1" - "9"] >
    |   <JS_EXP: ["e", "E"] ( <JS_PLUS > | <JS_MINUS > )? >
}
// JSON numbers do not support octal or hexadecimal formats
<IN_JSON> TOKEN: {
  
        <JS_NUMBER:  <JS_INTEGER> | <JS_INTEGER> <JS_FRACTIONAL_DIGITS> | <JS_INTEGER> <JS_EXPONENT> | <JS_INTEGER> <JS_FRACTIONAL_DIGITS> <JS_EXPONENT> >
    |   <JS_INTEGER: (<JS_MINUS>)? ( <JS_DIGIT> | <JS_NONZERO_DIGIT> <JS_DIGITS>) >
    |   <JS_FRACTIONAL_DIGITS: <JS_DOT> <JS_DIGITS > >
    |   <JS_EXPONENT: <JS_EXP> <JS_DIGITS> >
    |   <JS_DIGITS: ( <JS_DIGIT> )+ >
}

// string literals
<IN_JSON> TOKEN: {
        <JS_STRING: "\"" ( <JS_ALLOWABLE_CHARACTERS> )* "\"" >
    |   <JS_ALLOWABLE_CHARACTERS:(   
          (~["\"", "\\", "\u0000"-"\u001f"])
             | ("\\"
              ( ["u"] ["0"-"9","a"-"f", "A"-"F"] ["0"-"9","a"-"f", "A"-"F"] ["0"-"9","a"-"f", "A"-"F"] ["0"-"9","a"-"f", "A"-"F"]
                  | ["\"", "\\", "b", "f", "n", "r", "t"]
                  )
              )
          )
      >
}



JsonLiteral JsonLiteral():
{
JsonLiteral o = null;
}
{
    o=object()
    {
        return o;

    }
}

JsonLiteral object() :
{
  SqlParserPos pos;
  Map<String, Object> m = JsonLiteral.createNewMap();
}
{
    (<JS_OPENBRACE> | <LBRACE>)  { pos = getPos(); }
    ( members(m) )? <JS_CLOSEBRACE>
    {
        return new JsonLiteral(m, pos);
    }
}

void members(Map<String, Object> m) :
{
}
{
    pair(m) [ <JS_COMMA> members(m) ]
}

void pair(Map<String, Object> m) :
{
Token t = null;
Object o;
}
{
    t=<JS_STRING> <JS_COLON> o=value()
    {
        m.put(t.image, o);
    }
}

Object array() :
{
List<Object> a=new JsonStringArrayList<Object>();
}
{
    <JS_OPENBRACKET> ( elements(a) )? <JS_CLOSEBRACKET>
    {
        Collections.reverse(a);
        return a;
    }
}

void elements(List<Object> a) :
{
Object o = null;
}
{
    o=value() [ <JS_COMMA> elements(a) ]
    {
        a.add(o);
    }
}

Object value() :
{
Token t = null;
Object o = null;
}
{
    (   o=object()
    |   o=array() 
    |   t=<JS_STRING> {o = SqlParserUtil.trim(t.image, "\"'");}
    |   t=<JS_NUMBER>
        {
            try {
              o = Integer.valueOf(t.image);

            }
            catch (NumberFormatException nfe1) {
                try {
                    o = Long.valueOf(t.image);
                }
                catch (NumberFormatException nfe2) {
                    try {
                        o = Float.valueOf(t.image);
                    }
                    catch (NumberFormatException nfe3) {
                        try {
                            o = Double.valueOf(t.image);
                        }
                        catch  (NumberFormatException nfe4) {
                            o = Double.NaN;
                        }
                    }
                }

            }
        }
    |   <JS_TRUE> {o = Boolean.TRUE;}
    |   <JS_FALSE> {o = Boolean.TRUE;}
    |   <JS_NULL> )
    {
        return o;

    }
}
