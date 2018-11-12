package org.corfudb.universe.dynamic.events;

import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.function.DoubleSupplier;
import java.util.function.Supplier;

/**
 * Supplier function class used to describe the generation of a data for a corfu stream.
 * A generation of data is a simple map of field names and field values.
 *
 * Any child class of this MUST have a IDEMPOTENT behaviour. This is important to guaranty
 * the reproducibility of the generation of a data.
 * The idempotent behaviour is given by calls to {@link CorfuTableDataGenerationFunction::supplyData} under the same conditions of
 * the class.
 * If the generator is not a shnapshot, every call to {@link CorfuTableDataGenerationFunction::supplyData}
 * produce new values for the the fields.
 *
 * @param <K>   Type of the key generated.
 * @param <V>   Type of the value generated.
 *
 * Created by edilmo on 11/06/18.
 */
@Slf4j
public abstract class CorfuTableDataGenerationFunction<K, V> implements Supplier<Map<K, V>>, Cloneable {
    private static final String ALPHA_NUMERIC_STRING = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";

    /**
     * Generates a random alphanumeric string of the size specified using
     * the given random number generator.
     *
     * @param count                     Size of the string to produce.
     * @param randomDoubleSupplier     Random number between 0.0 and 1.0.
     * @return
     */
    private static String getRandomAlphaNumeric(int count, DoubleSupplier randomDoubleSupplier) {
        StringBuilder builder = new StringBuilder();
        while (count-- != 0) {
            int character = (int)(randomDoubleSupplier.getAsDouble() * ALPHA_NUMERIC_STRING.length());
            character %= ALPHA_NUMERIC_STRING.length();
            builder.append(ALPHA_NUMERIC_STRING.charAt(character));
        }
        return builder.toString();
    }

    /**
     * Integer id that is used as a simple seed to make each data generation function different
     * in a deterministic a reproducible way.
     */
    protected int id;

    /**
     * Amount of fields to generate
     */
    protected int fieldsCount;

    /**
     * Indicates if the instance of the data generator is a snapshot.
     * If the instance is a snapshot, every call to {@link CorfuTableDataGenerationFunction::get} will produce
     * the same result. This is used to validate data.
     * If the instance is not a snapshot, every call to {@link CorfuTableDataGenerationFunction::get} will not
     * produce the same values.
     */
    protected boolean isSnapshot = false;

    /**
     * Get the name of the data generator.
     *
     * @return Name of the data generator.
     */
    public abstract String getName();

    /**
     * Get the name of the stream table where the data generated will be saved in corfu.
     *
     * @return Name of the table stream in corfu.
     */
    public String getTableStreamName() {
        return this.getClass().getSimpleName() + this.id;
    }

    /**
     * Change internal conditions of the child class that
     * ensure new field values will be produced the next
     * time {@link CorfuTableDataGenerationFunction::supplyData} is invoked.
     */
    public abstract void changeValues();

    /**
     * Undo changes made by the last call to {@link CorfuTableDataGenerationFunction::changeValues}.
     */
    public abstract void undoChangeValues();

    /**
     * Produce the data that would be returned by {@link CorfuTableDataGenerationFunction::get}.
     * The method has a idempotent behaviour if the conditions
     * of the class are the same for each call.
     *
     * @return Data generated and returned by {@link CorfuTableDataGenerationFunction::get}.
     */
    public abstract Map<K, V> supplyData();

    /**
     * Get a snapshot of the class.
     * A snapshot of the class is a guaranty of a idempotent behaviour over all
     * calls to {@link CorfuTableDataGenerationFunction::get}.
     *
     * @return Snapshot of the class.
     * @throws CloneNotSupportedException
     */
    public CorfuTableDataGenerationFunction getSnapshot() throws CloneNotSupportedException {
        CorfuTableDataGenerationFunction snapshot = (CorfuTableDataGenerationFunction)this.clone();
        snapshot.isSnapshot = true;
        return snapshot;
    }

    /**
     * Generate the data.
     *
     * @return Generated data.
     */
    @Override
    public Map<K, V> get() {
        if(!this.isSnapshot)
            this.changeValues();
        Map<K, V> result = this.supplyData();
        return result;
    }

    private CorfuTableDataGenerationFunction() {

    }

    /**
     * Data generator that use a simple sequence of numbers as field names, and another sequence
     * as field values.
     * If the generator is not a shnapshot, every call to {@link IntegerSequence::supplyData} produce new values for the
     * the fields.
     */
    public final static class IntegerSequence extends CorfuTableDataGenerationFunction<String, String> {

        /**
         * Integer used to control the change over the values of the generator.
         */
        private int fieldValueDifference;

        /**
         * Produce the data that would be returned by {@link IntegerSequence::get}.
         * The method has a idempotent behaviour if the conditions
         * of the class are the same for each call.
         *
         * @return Data generated and returned by {@link IntegerSequence::get}.
         */
        @Override
        public Map<String, String> supplyData() {
            Map<String, String> result = new HashMap<String, String>();
            for(int i = this.id; i < (this.id + this.fieldsCount); i++){
                result.put(String.valueOf(i), String.valueOf(i - this.fieldValueDifference));
            }
            return result;
        }

        /**
         * Change the value of {@link IntegerSequence::fieldValueDifference} to
         * ensure new field values will be produced the next
         * time {@link IntegerSequence::supplyData} is invoked.
         */
        @Override
        public void changeValues() {
            if(!this.isSnapshot)
                this.fieldValueDifference += 1;
        }

        /**
         * Undo changes made by the last call to {@link IntegerSequence::changeValues}.
         */
        @Override
        public void undoChangeValues() {
            if(!this.isSnapshot)
                this.fieldValueDifference -= 1;
        }

        /**
         * Get name of the data generator.
         *
         * @return Name of the data generator.
         */
        @Override
        public String getName() {
            return "Integer Sequence";
        }

        public Object clone() {
            IntegerSequence clone = new IntegerSequence(this.id, this.fieldsCount, this.fieldValueDifference);
            return clone;
        }

        public IntegerSequence(int id, int fieldsCount){
            this(id, fieldsCount, 0);
        }

        private IntegerSequence(int id, int fieldsCount, int fieldValueDifference){
            this.id = id;
            this.fieldsCount = fieldsCount;
            this.fieldValueDifference = fieldValueDifference;
        }
    }

    /**
     * Data generator that use a random number generator to create field names and a simple sine function
     * to generate field values.
     * If the generator is not a shnapshot, every call to {@link IntegerSequence::supplyData} produce new values for the
     * the fields.
     */
    public final static class SinusoidalStrings extends CorfuTableDataGenerationFunction<String, String> {

        /**
         * Size of a random field name in amount of characters (a typical english word
         * is in avarage about 5 characters, and in avarage a field have a composed name
         * of 3 words).
         */
        private static final int FIELD_NAME_SIZE = 15;

        /**
         * Size of a random field value in amount of characters.
         */
        private static final int FIELD_VALUE_SIZE = 100;

        /**
         * Phase of the sine function used in {@link SinusoidalStrings::supplyData}.
         */
        private int initialPhase = 0;

        /**
         * Field names used by the generator.
         */
        private Set<String> fields;

        /**
         * Produce the data that would be returned by {@link SinusoidalStrings::get}.
         * The method has a idempotent behaviour if the conditions
         * of the class are the same for each call.
         *
         * @return Data generated and returned by {@link SinusoidalStrings::get}.
         */
        @Override
        public Map<String, String> supplyData() {
            Map<String, String> result = new HashMap<String, String>();
            Iterator fieldIterator = this.fields.iterator();
            for(int i = 0; i < this.fields.size(); i++){
                String field = (String)fieldIterator.next();
                int invFrequency = this.id * 30 * i;
                int phase = this.initialPhase + this.id + i;
                result.put(field, getRandomAlphaNumeric(FIELD_VALUE_SIZE, new DoubleSupplier() {
                    private int time = 0;
                    @Override
                    public double getAsDouble() {
                        double res = Math.abs(Math.sin(Math.toRadians((invFrequency * time) + phase)));
                        time++;
                        return res;
                    }
                }));
            }
            return result;
        }

        /**
         * Change internal value of the initialPhase of the sin to
         * ensure new field values will be produced the next
         * time {@link SinusoidalStrings::supplyData} is invoked.
         */
        @Override
        public void changeValues() {
            if(!this.isSnapshot)
                this.initialPhase += 1;
        }

        /**
         * Undo changes made by the last call to {@link SinusoidalStrings::changeValues}.
         */
        @Override
        public void undoChangeValues() {
            if(!this.isSnapshot)
                this.initialPhase -= 1;
        }

        /**
         * Get name of the data generator.
         *
         * @return Name of the data generator.
         */
        @Override
        public String getName() {
            return "Pseudo Random Strings";
        }

        public Object clone() {
            SinusoidalStrings clone = new SinusoidalStrings(this.id, this.fieldsCount, this.initialPhase, this.fields);
            return clone;
        }

        public SinusoidalStrings(int id, int fieldsCount){
            this(id, fieldsCount, 0, new HashSet<>());
        }

        private SinusoidalStrings(int id, int fieldsCount, int initialPhase, Set<String> fields){
            this.id = id;
            this.fieldsCount = fieldsCount;
            this.initialPhase = initialPhase;
            this.fields = fields;
            if(fields.isEmpty()){
                Random randomNumberGenerator = new Random(this.id);
                while (this.fields.size() != fieldsCount){
                    String potentialFieldName = getRandomAlphaNumeric(FIELD_NAME_SIZE, randomNumberGenerator::nextDouble);
                    this.fields.add(potentialFieldName);
                }
            }
        }
    }
}
