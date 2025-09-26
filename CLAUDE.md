# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is an event mesh system for secure, distributed event routing. See `event_mesh_design.md` for complete architecture and design details.

## Development Workflow

### Build Commands
- **Build solution**: `dotnet build` (from src/ directory)
- **Run tests**: `dotnet test` (from src/ directory)
- **Run specific test**: `dotnet test --filter "TestName"`
- **Clean**: `dotnet clean`

### Project Structure (Separated Interface Pattern)
```
src/
├── EventMesh.sln                    # Solution file
├── EventMesh.MessageContracts/      # Message interfaces (no dependencies)
│   ├── IEventRecord.cs
│   └── EventMesh.MessageContracts.csproj
├── EventMesh.Messages/              # Message implementations (depends on MessageContracts)
│   ├── EventRecord.cs (implements IEventRecord)
│   └── EventMesh.Messages.csproj
├── EventMesh.EventLog/              # Event Log service (depends on MessageContracts + Messages)
│   ├── IEventLog.cs (uses IEventRecord)
│   ├── InMemoryEventLog.cs
│   └── EventMesh.EventLog.csproj
└── EventMesh.EventLog.Tests/        # Event Log tests (depends on MessageContracts + Messages + EventLog)
    ├── UnitTest1.cs
    └── EventMesh.EventLog.Tests.csproj
```

### Dependency Flow
- **MessageContracts**: No dependencies (pure message interfaces)
- **Messages**: MessageContracts (message implementations)
- **EventLog**: MessageContracts + Messages (service interfaces + implementations)
- **Tests**: MessageContracts + Messages + EventLog (can mock via interfaces)

**Benefits of Separated Interface Pattern:**
- **Proper Separation**: Message contracts separate from service contracts
- **No Circular Dependencies**: Clean unidirectional dependency flow
- **Interface-Based Testing**: Tests can mock `IEventRecord` and `IEventLog`
- **Service Co-location**: `IEventLog` lives with its implementations in EventLog
- **Message Abstraction**: `IEventRecord` provides stable contract for different message types

## Key References

- **Design Document**: `event_mesh_design.md` - Complete system design, architecture, and requirements
- **License**: MIT License (see LICENSE file)

## Development Guidelines

### Test-Driven Development (TDD)
This project follows strict TDD practices:
- **Red-Green-Refactor cycle**: Write failing test first, make it pass with minimal code, then refactor
- **No implementation without tests**: All code must be driven by failing tests
- **Test coverage**: Every function, method, and component must have comprehensive tests
- **Tests as specification**: Tests should clearly document expected behavior and edge cases
- **Frequent commits**: Check in after each complete red-green-refactor cycle to maintain clean history

### Small Batch Development
- Work in small, focused batches (single feature or component at a time)
- Complete one small piece fully (test + implementation + refactor) before moving to the next
- Avoid large, multi-component changes that are hard to review and debug

### Simplicity First
- **Keep implementation simple**: Favor clear, straightforward solutions over clever optimizations
- **YAGNI (You Aren't Gonna Need It)**: Don't add features or complexity until actually needed
- **Readable code**: Code should be self-documenting and easy to understand
- **Easy to refactor**: Simple code is easier to modify when requirements change
- **Maintainability over performance**: Optimize for developer productivity and code clarity first

### Technical Requirements
- Follow the architecture and requirements defined in `event_mesh_design.md`
- Use the technical decisions documented there (RocksDB for storage, gRPC/mTLS for communication)
- Implement components according to the detailed requirements (REQ-LOG-001, REQ-RT-001, etc.)
- Maintain the security requirements (mTLS, client authentication)

## Future Updates

As development progresses, this file should be updated with:
- Build and test commands
- Development environment setup
- Code organization patterns
- Common debugging procedures