package mondrian.rolap;

public class PreEvalFailedException extends RuntimeException {

    public static final PreEvalFailedException INSTANCE =
        new PreEvalFailedException();

    private PreEvalFailedException() {
    }
	
}
