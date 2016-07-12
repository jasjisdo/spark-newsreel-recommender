package de.dailab.newsreel.recommender.common.util;

import org.apache.commons.lang.NullArgumentException;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
 * Created by jens on 02.03.16.
 */
public class EvalLevelTest {

    public static void main(String[] args) {
        Logger logger = Logger.getLogger(EvalLevel.class);
        logger.setLevel(EvalLevel.EVAL);
        Throwable _throw = new NullArgumentException("some npe");
        logger.log(EvalLevel.EVAL, "Eval log", _throw);
        logger.log(Level.DEBUG, "I am a DEBUG message");
        logger.log(Level.INFO, "I am a INFO message");
        logger.log(Level.FATAL, "I am a FATAL message");
    }

}