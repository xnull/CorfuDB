package org.corfudb.test.console;

import com.google.common.collect.ImmutableList;
import java.util.List;
import org.fusesource.jansi.Ansi;

public class Spinner implements IConsoleAnimation {
    int glyph = 0;

    List<String> glyphs = ImmutableList.<String>builder()
        .add("⠋")
        .add("⠙")
        .add("⠹")
        .add("⠸")
        .add("⠼")
        .add("⠴")
        .add("⠦")
        .add("⠧")
        .add("⠇")
        .add("⠏")
        .build();

    @Override
    public int animate() {
        draw(glyph);
        glyph++;
        if (glyph == glyphs.size()) {
            glyph = 0;
        }
        return 1;
    }

    void draw(int glyph) {
        System.out.print(Ansi.ansi()
            .a(glyphs.get(glyph)));
        System.out.flush();
    }
}
