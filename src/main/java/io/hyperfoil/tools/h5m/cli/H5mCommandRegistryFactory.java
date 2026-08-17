package io.hyperfoil.tools.h5m.cli;

import java.util.Set;

import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Alternative;
import jakarta.enterprise.inject.Instance;

import org.aesh.command.Command;
import org.aesh.command.DefaultValueProvider;
import org.aesh.command.impl.registry.AeshCommandRegistryBuilder;
import org.aesh.command.invocation.CommandInvocation;

import io.quarkus.aesh.runtime.AeshCdiCommandContainerBuilder;
import io.quarkus.aesh.runtime.CliCommandRegistryFactory;

/**
 * Custom command registry factory that only registers top-level commands.
 * Subcommands (referenced via groupCommands in parent @CommandDefinition)
 * are discovered automatically by aesh from the parent command.
 * <p>
 * This prevents subcommands from appearing as top-level commands in tab completion.
 */
@Alternative
@Priority(1)
@ApplicationScoped
public class H5mCommandRegistryFactory implements CliCommandRegistryFactory {

    /** Top-level command classes — only these are registered in the registry */
    private static final Set<Class<?>> TOP_LEVEL_COMMANDS = Set.of(
            FolderCmd.class,
            NodeCmd.class,
            NotificationCmd.class,
            LegacyCmd.class,
            AdminCmd.class,
            ChangeFolderCmd.class,
            StatusCmd.class,
            ChangesCmd.class,
            ViewCmd.class,
            Veritaserum.class,
            RunCmd.class
    );

    private final Instance<Command<? extends CommandInvocation>> commands;
    private final Instance<DefaultValueProvider> defaultValueProvider;

    @SuppressWarnings("unchecked")
    public H5mCommandRegistryFactory(Instance<Command<? extends CommandInvocation>> commands,
            Instance<DefaultValueProvider> defaultValueProvider) {
        this.commands = commands;
        this.defaultValueProvider = defaultValueProvider;
    }

    @Override
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public AeshCommandRegistryBuilder create() {
        AeshCommandRegistryBuilder builder = AeshCommandRegistryBuilder.builder();
        builder.containerBuilder(new AeshCdiCommandContainerBuilder<>());

        if (defaultValueProvider.isResolvable()) {
            builder.defaultValueProvider(defaultValueProvider.get());
        }

        for (Command command : commands) {
            if (TOP_LEVEL_COMMANDS.contains(command.getClass())) {
                try {
                    builder.command(command);
                } catch (Exception e) {
                    throw new RuntimeException(
                            "Failed to register command: " + command.getClass().getName(), e);
                }
            }
        }

        return builder;
    }
}
