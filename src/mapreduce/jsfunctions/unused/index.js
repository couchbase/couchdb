/**
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

var esprima = require('esprima');

var Context = require('./lib/context');

// return a list of unused variables in the source
function unused(src) {
    var ast = esprima.parse(src, {
        loc: true
    });

    // map of identifiers -> location
    // when an identifier is seen, it is removed from the map
    var unused_vars = [];

    function exec(node, context) {
        if (!node) {
            return;
        }

        handlers[node.type](node, context);
    };

    function maybe_set_id(id, context, is_param) {
        if (!id) {
            return;
        }

        if (id.type !== 'Identifier') {
            exec(id, context);
            return;
        }

        context.set(id.name, {
            name: id.name,
            loc: id.loc.start,
            param: is_param || false
        });
    }

    function maybe_set_param(id, context) {
        maybe_set_id(id, context, true);
    }

    var handlers = {
        VariableDeclaration: function(node, context) {
            node.declarations.forEach(function(node) {
                exec(node, context)
            });
        },
        VariableDeclarator: function(node, context) {
            maybe_set_id(node.id, context);
            exec(node.init, context);
        },
        FunctionExpression: function(node, context) {
            // function express ids are ignored
            // assume user specified it for backtrace reasons

            var ctx = new Context(context);

            // parameters are within the context of the function
            node.params.forEach(function(node) {
                maybe_set_param(node, ctx);
            });

            // exec function body with new context
            exec(node.body, ctx);

            Array.prototype.push.apply(unused_vars, ctx.unused());
        },
        FunctionDeclaration: function(node, context) {
            maybe_set_id(node.id, context);

            var ctx = new Context(context);

            // parameters are within the context of the function
            node.params.forEach(function(node) {
                maybe_set_param(node, ctx);
            });

            // exec function body with new context
            exec(node.body, ctx);

            Array.prototype.push.apply(unused_vars, ctx.unused());
        },
        BlockStatement: function(node, context) {
            node.body.forEach(function(node) {
                exec(node, context);
            });
        },
        CallExpression: function(node, context) {
            exec(node.callee, context);

            node.arguments.forEach(function(node) {
                exec(node, context);
            });
        },
        MemberExpression: function(node, context) {
            exec(node.object, context);
            exec(node.property, context);
        },
        ExpressionStatement: function(node, context) {
            exec(node.expression, context);
        },
        ObjectExpression: function(node, context) {
            node.properties.forEach(function(node) {
                exec(node, context);
            });
        },
        AssignmentExpression: function(node, context) {
            exec(node.left, context);
            exec(node.right, context);
        },
        LogicalExpression: function(node, context) {
            exec(node.left, context);
            exec(node.right, context);
        },
        BinaryExpression: function(node, context) {
            exec(node.left, context);
            exec(node.right, context);
        },
        TryStatement: function(node, context) {
            node.block.body.forEach(function(node) {
                exec(node, context);
            });

            node.handlers.forEach(function(node) {
                exec(node, context);
            });
        },
        CatchClause: function(node, context) {
            exec(node.param, context);
            exec(node.body, context);
        },
        ConditionalExpression: function(node, context) {
            exec(node.test, context);
            exec(node.consequent, context);
            exec(node.alternate, context);
        },
        ArrayExpression: function(node, context) {
            node.elements.forEach(function(node) {
                exec(node, context);
            });
        },
        UpdateExpression: function(node, context) {
            exec(node.argument, context);
        },
        UnaryExpression: function(node, context) {
            exec(node.argument, context);
        },
        ThrowStatement: function(node, context) {
            exec(node.argument, context);
        },
        IfStatement: function(node, context) {
            exec(node.test, context);
            exec(node.consequent, context);
            exec(node.alternate, context);
        },
        ReturnStatement: function(node, context) {
            exec(node.argument, context);
        },
        SwitchStatement: function(node, context) {
            exec(node.discriminant, context);
            node.cases.forEach(function(node) {
                exec(node, context);
            });
        },
        SwitchCase: function(node, context) {
            exec(node.test, context);
            node.consequent.forEach(function(node) {
                exec(node, context);
            });
        },
        NewExpression: function(node, context) {
            exec(node.callee, context);
            node.arguments.forEach(function(node) {
                exec(node, context);
            });
        },
        Property: function(node, context) {
            exec(node.key, context);
            exec(node.value, context);
        },
        ForInStatement: function(node, context) {
            exec(node.left, context);
            exec(node.right, context);
            exec(node.body, context);
        },
        WhileStatement: function(node, context) {
            exec(node.test, context);
            exec(node.body, context);
        },
        SequenceExpression: function(node, context) {
            node.expressions.forEach(function(node) {
                exec(node, context);
            });
        },
        ForStatement: function(node, context) {
            exec(node.init, context);
            exec(node.test, context);
            exec(node.update, context);
            exec(node.body, context);
        },
        DoWhileStatement: function(node, context) {
            exec(node.body, context);
            exec(node.test, context);
        },
        ContinueStatement: function() {
        },
        BreakStatement: function() {
        },
        ThisExpression: function() {
        },
        EmptyStatement: function() {
        },
        Literal: function() {
        },
        Identifier: function(node, context) {
            context.remove(node.name);
        },
    };

    // TODO handle the case where use of a variable comes before declaring it
    // while this seems retarded, javascript does allow it

    var body = ast.body;

    var ctx = new Context();

    body.forEach(function(node) {
        exec(node, ctx);
    });

    Array.prototype.push.apply(unused_vars, ctx.unused());

    return unused_vars;
}

module.exports = unused;


