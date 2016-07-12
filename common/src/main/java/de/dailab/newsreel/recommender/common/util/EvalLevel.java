package de.dailab.newsreel.recommender.common.util;

import org.apache.log4j.Level;

/**
 * Created by jens on 02.03.16.
 */
public class EvalLevel extends Level {

    /**
     * This Level is lower than all normal levels, therefor it will not appear in any other loggers
     */
    public static final int EVAL_INT = 1;

    private static final String NAME = "EVAL";

    /**
     * Level representing my log level
     */
    public static final Level EVAL = new EvalLevel(EVAL_INT, NAME, 10);



    /**
     * Instantiate a Level object.
     *
     * @param level
     * @param levelStr
     * @param syslogEquivalent
     */
    protected EvalLevel(int level, String levelStr, int syslogEquivalent) {
        super(level, levelStr, syslogEquivalent);
    }

    /**
     * Checks whether logArgument is "EVAL" level. If yes then returns
     * EVAL}, else calls EvalLevel#toLevel(String, Level) passing
     * it Level#DEBUG as the defaultLevel.
     */
    public static Level toLevel(String logArgument) {
        if (logArgument != null && logArgument.toUpperCase().equals(NAME)) {
            return EVAL;
        }
        return (Level) toLevel(logArgument);
    }

    /**
     * Checks whether val is EvalLevel#EVAL_INT. If yes then
     * returns EvalLevel#EVAL, else calls
     * EvalLevel#toLevel(int, Level) passing it Level#DEBUG as the
     * defaultLevel
     *
     */
    public static Level toLevel(int val) {
        if (val == EVAL_INT) {
            return EVAL;
        }
        return (Level) toLevel(val, Level.DEBUG);
    }

    /**
     * Checks whether val is EvalLevel#EVAL_INT. If yes
     * then returns EvalLevel#EVAL, else calls Level#toLevel(int, org.apache.log4j.Level)
     *
     */
    public static Level toLevel(int val, Level defaultLevel) {
        if (val == EVAL_INT) {
            return EVAL;
        }
        return Level.toLevel(val, defaultLevel);
    }

    /**
     * Checks whether logArgument is "EVAL" level. If yes then returns
     * EvalLevel#EVAL, else calls
     * Level#toLevel(java.lang.String, org.apache.log4j.Level)
     *
     */
    public static Level toLevel(String logArgument, Level defaultLevel) {
        if (logArgument != null && logArgument.toUpperCase().equals(NAME)) {
            return EVAL;
        }
        return Level.toLevel(logArgument, defaultLevel);
    }
}
