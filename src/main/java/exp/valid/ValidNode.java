package exp.valid;

import jakarta.validation.Constraint;
import jakarta.validation.Payload;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target({ElementType.TYPE_USE, ElementType.FIELD})
@Retention(RetentionPolicy.RUNTIME)
@Constraint(validatedBy = {NodeValidator.class})
public @interface ValidNode {

    String message() default "Node's cannot have sources that loop back onto themselves";

    Class<?>[] groups() default {};

    Class<? extends Payload>[] payload() default {};

}
