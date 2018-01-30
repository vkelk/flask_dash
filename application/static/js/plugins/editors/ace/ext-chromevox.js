define("ace/ext/chromevox", ["require", "exports", "module", "ace/editor", "ace/config"], function (e, t, n) {
    function gt() {
        return typeof cvox != "undefined" && cvox && cvox.Api
    }

    function wt(e) {
        if (gt()) mt(e); else {
            yt++;
            if (yt >= bt) return;
            window.setTimeout(wt, 500, e)
        }
    }

    var r = {};
    r.SpeechProperty, r.Cursor, r.Token, r.Annotation;
    var i = {rate: .8, pitch: .4, volume: .9}, s = {rate: 1, pitch: .5, volume: .9},
        o = {rate: .8, pitch: .8, volume: .9}, u = {rate: .8, pitch: .3, volume: .9},
        a = {rate: .8, pitch: .7, volume: .9}, f = {rate: .8, pitch: .8, volume: .9},
        l = {punctuationEcho: "none", relativePitch: -0.6}, c = "ALERT_NONMODAL", h = "ALERT_MODAL",
        p = "INVALID_KEYPRESS", d = "insertMode", v = "start",
        m = [{substr: ";", newSubstr: " semicolon "}, {substr: ":", newSubstr: " colon "}], g = {
            SPEAK_ANNOT: "annots",
            SPEAK_ALL_ANNOTS: "all_annots",
            TOGGLE_LOCATION: "toggle_location",
            SPEAK_MODE: "mode",
            SPEAK_ROW_COL: "row_col",
            TOGGLE_DISPLACEMENT: "toggle_displacement",
            FOCUS_TEXT: "focus_text"
        }, y = "CONTROL + SHIFT ";
    r.editor = null;
    var b = null, w = {}, E = !1, S = !1, x = !1, T = null, N = {}, C = {}, k = function (e) {
        return y + String.fromCharCode(e)
    }, L = function () {
        var e = r.editor.keyBinding.getKeyboardHandler();
        return e.$id === "ace/keyboard/vim"
    }, A = function (e) {
        return r.editor.getSession().getTokenAt(e.row, e.column + 1)
    }, O = function (e) {
        return r.editor.getSession().getLine(e.row)
    }, M = function (e) {
        w[e.row] && cvox.Api.playEarcon(c), E ? (cvox.Api.stop(), W(e), R(A(e)), I(e.row, 1)) : I(e.row, 0)
    }, _ = function (e) {
        var t = O(e), n = t.substr(e.column - 1);
        e.column === 0 && (n = " " + t);
        var r = /^\W(\w+)/, i = r.exec(n);
        return i !== null
    }, D = {
        constant: {prop: i},
        entity: {prop: o},
        keyword: {prop: u},
        storage: {prop: a},
        variable: {prop: f},
        meta: {
            prop: s,
            replace: [{substr: "</", newSubstr: " closing tag "}, {
                substr: "/>",
                newSubstr: " close tag "
            }, {substr: "<", newSubstr: " tag start "}, {substr: ">", newSubstr: " tag end "}]
        }
    }, P = {prop: P}, H = function (e, t) {
        var n = e;
        for (var r = 0; r < t.length; r++) {
            var i = t[r], s = new RegExp(i.substr, "g");
            n = n.replace(s, i.newSubstr)
        }
        return n
    }, B = function (e, t, n) {
        var r = {};
        r.value = "", r.type = e[t].type;
        for (var i = t; i < n; i++) r.value += e[i].value;
        return r
    }, j = function (e) {
        if (e.length <= 1) return e;
        var t = [], n = 0;
        for (var r = 1; r < e.length; r++) {
            var i = e[n], s = e[r];
            U(i) !== U(s) && (t.push(B(e, n, r)), n = r)
        }
        return t.push(B(e, n, e.length)), t
    }, F = function (e) {
        var t = r.editor.getSession().getLine(e), n = /^\s*$/;
        return n.exec(t) !== null
    }, I = function (e, t) {
        var n = r.editor.getSession().getTokens(e);
        if (n.length === 0 || F(e)) {
            cvox.Api.playEarcon("EDITABLE_TEXT");
            return
        }
        n = j(n);
        var i = n[0];
        n = n.filter(function (e) {
            return e !== i
        }), z(i, t), n.forEach(R)
    }, q = function (e) {
        z(e, 0)
    }, R = function (e) {
        z(e, 1)
    }, U = function (e) {
        if (!e || !e.type) return;
        var t = e.type.split(".");
        if (t.length === 0) return;
        var n = t[0], r = D[n];
        return r ? r : P
    }, z = function (e, t) {
        var n = U(e), r = H(e.value, m);
        n.replace && (r = H(r, n.replace)), cvox.Api.speak(r, t, n.prop)
    }, W = function (e) {
        var t = O(e);
        cvox.Api.speak(t[e.column], 1)
    }, X = function (e, t) {
        var n = O(t), r = n.substring(e.column, t.column);
        r = r.replace(/ /g, " space "), cvox.Api.speak(r)
    }, V = function (e, t) {
        if (Math.abs(e.column - t.column) !== 1) {
            var n = O(t).length;
            if (t.column === 0 || t.column === n) {
                I(t.row, 0);
                return
            }
            if (_(t)) {
                cvox.Api.stop(), R(A(t));
                return
            }
        }
        W(t)
    }, $ = function (e, t) {
        r.editor.selection.isEmpty() ? S ? X(e, t) : V(e, t) : (X(e, t), cvox.Api.speak("selected", 1))
    }, J = function (e) {
        if (x) {
            x = !1;
            return
        }
        var t = r.editor.selection.getCursor();
        t.row !== b.row ? M(t) : $(b, t), b = t
    }, K = function (e) {
        r.editor.selection.isEmpty() && cvox.Api.speak("unselected")
    }, Q = function (e) {
        var t = e.data;
        switch (t.action) {
            case"removeText":
                cvox.Api.speak(t.text, 0, l), x = !0;
                break;
            case"insertText":
                cvox.Api.speak(t.text, 0), x = !0
        }
    }, G = function (e) {
        var t = e.row, n = e.column;
        return !w[t] || !w[t][n]
    }, Y = function (e) {
        w = {};
        for (var t = 0; t < e.length; t++) {
            var n = e[t], r = n.row, i = n.column;
            w[r] || (w[r] = {}), w[r][i] = n
        }
    }, Z = function (e) {
        var t = r.editor.getSession().getAnnotations(), n = t.filter(G);
        n.length > 0 && cvox.Api.playEarcon(c), Y(t)
    }, et = function (e) {
        var t = e.type + " " + e.text + " on " + nt(e.row, e.column);
        t = t.replace(";", "semicolon"), cvox.Api.speak(t, 1)
    }, tt = function (e) {
        var t = w[e];
        for (var n in t) et(t[n])
    }, nt = function (e, t) {
        return "row " + (e + 1) + " column " + (t + 1)
    }, rt = function () {
        cvox.Api.speak(nt(b.row, b.column))
    }, it = function () {
        for (var e in w) tt(e)
    }, st = function () {
        if (!L()) return;
        switch (r.editor.keyBinding.$data.state) {
            case d:
                cvox.Api.speak("Insert mode");
                break;
            case v:
                cvox.Api.speak("Command mode")
        }
    }, ot = function () {
        E = !E, E ? cvox.Api.speak("Speak location on row change enabled.") : cvox.Api.speak("Speak location on row change disabled.")
    }, ut = function () {
        S = !S, S ? cvox.Api.speak("Speak displacement on column changes.") : cvox.Api.speak("Speak current character or word on column changes.")
    }, at = function (e) {
        if (e.ctrlKey && e.shiftKey) {
            var t = N[e.keyCode];
            t && t.func()
        }
    }, ft = function (e, t) {
        if (!L()) return;
        var n = t.keyBinding.$data.state;
        if (n === T) return;
        switch (n) {
            case d:
                cvox.Api.playEarcon(h), cvox.Api.setKeyEcho(!0);
                break;
            case v:
                cvox.Api.playEarcon(h), cvox.Api.setKeyEcho(!1)
        }
        T = n
    }, lt = function (e) {
        var t = e.detail.customCommand, n = C[t];
        n && (n.func(), r.editor.focus())
    }, ct = function () {
        var e = dt.map(function (e) {
            return {desc: e.desc + k(e.keyCode), cmd: e.cmd}
        }), t = document.querySelector("body");
        t.setAttribute("contextMenuActions", JSON.stringify(e)), t.addEventListener("ATCustomEvent", lt, !0)
    }, ht = function (e) {
        e.match ? I(b.row, 0) : cvox.Api.playEarcon(p)
    }, pt = function () {
        r.editor.focus()
    }, dt = [{
        keyCode: 49, func: function () {
            tt(b.row)
        }, cmd: g.SPEAK_ANNOT, desc: "Speak annotations on line"
    }, {keyCode: 50, func: it, cmd: g.SPEAK_ALL_ANNOTS, desc: "Speak all annotations"}, {
        keyCode: 51,
        func: st,
        cmd: g.SPEAK_MODE,
        desc: "Speak Vim mode"
    }, {keyCode: 52, func: ot, cmd: g.TOGGLE_LOCATION, desc: "Toggle speak row location"}, {
        keyCode: 53,
        func: rt,
        cmd: g.SPEAK_ROW_COL,
        desc: "Speak row and column"
    }, {keyCode: 54, func: ut, cmd: g.TOGGLE_DISPLACEMENT, desc: "Toggle speak displacement"}, {
        keyCode: 55,
        func: pt,
        cmd: g.FOCUS_TEXT,
        desc: "Focus text"
    }], vt = function () {
        r.editor = editor, editor.getSession().selection.on("changeCursor", J), editor.getSession().selection.on("changeSelection", K), editor.getSession().on("change", Q), editor.getSession().on("changeAnnotation", Z), editor.on("changeStatus", ft), editor.on("findSearchBox", ht), editor.container.addEventListener("keydown", at), b = editor.selection.getCursor()
    }, mt = function (e) {
        vt(), dt.forEach(function (e) {
            N[e.keyCode] = e, C[e.cmd] = e
        }), e.on("focus", vt), L() && cvox.Api.setKeyEcho(!1), ct()
    }, yt = 0, bt = 15, Et = e("../editor").Editor;
    e("../config").defineOptions(Et.prototype, "editor", {
        enableChromevoxEnhancements: {
            set: function (e) {
                e && wt(this)
            }, value: !0
        }
    })
});
(function () {
    window.require(["ace/ext/chromevox"], function () {
    });
})();
            