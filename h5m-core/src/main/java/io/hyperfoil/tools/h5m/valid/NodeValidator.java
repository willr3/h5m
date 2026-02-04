package io.hyperfoil.tools.h5m.valid;

import io.hyperfoil.tools.h5m.entity.Node;
import jakarta.validation.ConstraintValidator;
import jakarta.validation.ConstraintValidatorContext;


public class NodeValidator implements ConstraintValidator<ValidNode, Node> {

    @Override
    public void initialize(ValidNode constraintAnnotation) {
        ConstraintValidator.super.initialize(constraintAnnotation);
    }


    @Override
    public boolean isValid(Node node, ConstraintValidatorContext constraintValidatorContext) {
        boolean rtrn = true;
        if(node == null){
            constraintValidatorContext.buildConstraintViolationWithTemplate("null nodes are not valid")
                    .addConstraintViolation();
            return false;
        }
        if(node.isCircular()){
            constraintValidatorContext.buildConstraintViolationWithTemplate(
                    "nodes cannot create a circular reference with other nodes").addConstraintViolation();
            rtrn = false;
        }
        if(node.name == null || node.name.isEmpty()){
            constraintValidatorContext.buildConstraintViolationWithTemplate(
                    "node names cannot be empty"
            ).addConstraintViolation();
            rtrn = false;
        }
        if(node.name.matches("^\\d+")){
            constraintValidatorContext.buildConstraintViolationWithTemplate(
                    "node names cannot be numbers"
            ).addConstraintViolation();
            rtrn = false;
        }
        if(node.name.contains(Node.FQDN_SEPARATOR)){
            constraintValidatorContext.buildConstraintViolationWithTemplate(
                    "node names cannot contain "+Node.FQDN_SEPARATOR+" characters"
            ).addConstraintViolation();
            rtrn = false;
        }


        return rtrn;
    }
}
